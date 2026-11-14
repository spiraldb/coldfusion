// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Iceberg field IDs, evolution, defaults, and partition constants.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    Literal, MappedField, NestedFieldRef, PrimitiveLiteral, Schema, SchemaRef, StructType,
    Transform, Type,
};
use iceberg::{Error, ErrorKind, Result};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::{DType, Nullability};
use vortex::expr::{Expression, case_when, cast, get_item, is_null, lit, pack, root};
use vortex::scalar::{DecimalValue, Scalar};
use vortex::session::VortexSession;

use super::{nested, to_iceberg_error};

pub(super) fn file_schema(arrow: ArrowSchemaRef, task: &FileScanTask) -> Result<ArrowSchemaRef> {
    if arrow
        .fields()
        .iter()
        .any(|f| f.metadata().contains_key(PARQUET_FIELD_ID_META_KEY))
    {
        return Ok(arrow);
    }
    fn mapped(field: &FieldRef, entry: Option<&MappedField>, fallback: Option<i32>) -> FieldRef {
        fn mapped_child<'a>(entry: Option<&'a MappedField>, name: &str) -> Option<&'a MappedField> {
            entry.and_then(|entry| {
                entry
                    .fields()
                    .iter()
                    .find(|f| f.names().iter().any(|n| n == name))
                    .map(Arc::as_ref)
            })
        }
        let id = entry.and_then(MappedField::field_id).or(fallback);
        let dtype = match field.data_type() {
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|child| mapped(child, mapped_child(entry, child.name()), None))
                    .collect(),
            ),
            DataType::List(element) => {
                DataType::List(mapped(element, mapped_child(entry, "element"), None))
            }
            DataType::LargeList(element) => {
                DataType::LargeList(mapped(element, mapped_child(entry, "element"), None))
            }
            DataType::FixedSizeList(element, size) => DataType::FixedSizeList(
                mapped(element, mapped_child(entry, "element"), None),
                *size,
            ),
            DataType::Map(entries, ordered) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return field.clone();
                };
                let fields = fields
                    .iter()
                    .map(|f| mapped(f, mapped_child(entry, f.name()), None))
                    .collect();
                DataType::Map(
                    Arc::new(
                        entries
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Struct(fields)),
                    ),
                    *ordered,
                )
            }
            other => other.clone(),
        };
        let mut field = field.as_ref().clone().with_data_type(dtype);
        if let Some(id) = id {
            field
                .metadata_mut()
                .insert(PARQUET_FIELD_ID_META_KEY.to_owned(), id.to_string());
        }
        Arc::new(field)
    }
    let fields: Fields = arrow
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let mapping = task
                .name_mapping()
                .and_then(|m| m.fields().iter().find(|m| m.names().contains(field.name())));
            mapped(
                field,
                mapping,
                task.name_mapping().is_none().then_some((i + 1) as i32),
            )
        })
        .collect();
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    )))
}

pub(super) fn field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

/// Finds a primitive or struct field by ID without traversing repeated list/map values.
pub(super) fn field_path(fields: &Fields, id: i32) -> Option<Vec<FieldRef>> {
    for field in fields {
        if field_id(field) == Some(id) {
            return Some(vec![field.clone()]);
        }
        if let DataType::Struct(children) = field.data_type()
            && let Some(mut path) = field_path(children, id)
        {
            path.insert(0, field.clone());
            return Some(path);
        }
    }
    None
}

/// Retains missing equality keys under their historical struct ancestors.
pub(super) fn delete_read_schema(
    schema: &SchemaRef,
    history: &[SchemaRef],
    ids: impl Iterator<Item = i32>,
) -> Result<SchemaRef> {
    fn path(fields: &[NestedFieldRef], id: i32) -> Option<Vec<NestedFieldRef>> {
        for field in fields {
            if field.id == id {
                return Some(vec![field.clone()]);
            }
            if let Type::Struct(children) = field.field_type.as_ref()
                && let Some(mut result) = path(children.fields(), id)
            {
                result.insert(0, field.clone());
                return Some(result);
            }
        }
        None
    }
    fn insert(fields: &mut Vec<NestedFieldRef>, path: &[NestedFieldRef], key: i32) -> Result<()> {
        if let Some(index) = fields.iter().position(|field| field.id == path[0].id) {
            if path.len() > 1 {
                let field = Arc::make_mut(&mut fields[index]);
                let Type::Struct(children) = field.field_type.as_ref() else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Equality key ancestor is not a struct",
                    ));
                };
                let mut nested = children.fields().to_vec();
                insert(&mut nested, &path[1..], key)?;
                *field.field_type = Type::Struct(StructType::new(nested));
            }
        } else {
            let mut field = path[0].as_ref().clone();
            // A non-null struct default is populated from its subfields' defaults,
            // so retain those definitions even when only one delete key is read.
            field.name = format!("__coldfusion_delete_key_{key}");
            while fields.iter().any(|existing| existing.name == field.name) {
                field.name.push('_');
            }
            fields.push(Arc::new(field));
        }
        Ok(())
    }
    let mut fields = schema.as_struct().fields().to_vec();
    let mut seen = HashSet::new();
    for id in ids {
        if schema.field_by_id(id).is_some() || !seen.insert(id) {
            continue;
        }
        let historical = history
            .iter()
            .filter(|schema| schema.field_by_id(id).is_some())
            .max_by_key(|schema| schema.schema_id())
            .and_then(|schema| path(schema.as_struct().fields(), id))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Unknown or repeated equality-delete field {id}"),
                )
            })?;
        insert(&mut fields, &historical, id)?;
    }
    if seen.is_empty() {
        return Ok(schema.clone());
    }
    Ok(Arc::new(Schema::builder().with_fields(fields).build()?))
}

pub(super) fn normalization(
    task: &FileScanTask,
    read_schema: &Schema,
    physical: &ArrowSchema,
    session: &VortexSession,
) -> Result<Expression> {
    let target = schema_to_arrow_schema(read_schema)?;
    let mut defaults = HashMap::new();
    for target in all_fields(target.fields()) {
        if let Some(field) = field_id(&target)
            .and_then(|id| read_schema.field_by_id(id))
            .filter(|field| field.initial_default.is_some())
        {
            defaults.insert(field.id, field.clone());
        }
    }
    let defaults = Arc::new(defaults);
    let mut constants = Vec::new();
    if let (Some(spec), Some(partition)) = (task.partition_spec(), task.partition()) {
        for (index, field) in spec.fields().iter().enumerate() {
            if matches!(field.transform, Transform::Identity) {
                let value = partition.fields().get(index).cloned().flatten();
                constants.push((field.source_id, value));
            }
        }
    }
    fn fields(
        ice: &[NestedFieldRef],
        output: &Fields,
        physical: &Fields,
        base: Expression,
        session: &VortexSession,
        constants: &[(i32, Option<Literal>)],
        defaults: &Arc<HashMap<i32, NestedFieldRef>>,
    ) -> Result<Expression> {
        let values = ice
            .iter()
            .zip(output)
            .map(|(field, target)| {
                let target_dtype = session
                    .arrow()
                    .from_arrow_field(target)
                    .map_err(to_iceberg_error)?;
                let constant = constants.iter().find(|(id, _)| *id == field.id);
                let physical = field_path(physical, field.id);
                let value = if let Some((_, value)) = constant {
                    match value.as_ref() {
                        Some(value) => literal(value, &target_dtype)?,
                        None => lit(Scalar::null(target_dtype.clone())),
                    }
                } else if let Some(path) = physical {
                    let physical = path.last().unwrap();
                    let expr = path.iter().fold(base.clone(), |input, field| {
                        get_item(field.name().as_str(), input)
                    });
                    if let (
                        Type::Struct(children),
                        DataType::Struct(target_fields),
                        DataType::Struct(physical_fields),
                    ) = (
                        field.field_type.as_ref(),
                        target.data_type(),
                        physical.data_type(),
                    ) {
                        // Preserve nullable parents while matching children by field ID.
                        let nested = fields(
                            children.fields(),
                            target_fields,
                            physical_fields,
                            expr.clone(),
                            session,
                            constants,
                            defaults,
                        )?;
                        if physical.is_nullable() {
                            let null = lit(Scalar::null(target_dtype.clone()));
                            case_when(is_null(expr), null, cast(nested, target_dtype.clone()))
                        } else {
                            cast(nested, target_dtype.clone())
                        }
                    } else if matches!(field.field_type.as_ref(), Type::List(_) | Type::Map(_))
                        && physical.data_type() != target.data_type()
                    {
                        nested::transform(
                            expr,
                            physical.clone(),
                            target.clone(),
                            defaults.clone(),
                            session,
                        )?
                    } else {
                        cast(expr, target_dtype.clone())
                    }
                } else if field.initial_default.is_some() {
                    lit(initial_default(field, &target_dtype)?)
                } else if !field.required {
                    lit(Scalar::null(target_dtype.clone()))
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Required Iceberg field {} is missing", field.name),
                    ));
                };
                Ok((field.name.clone(), value))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(pack(values, Nullability::NonNullable))
    }
    fields(
        read_schema.as_struct().fields(),
        target.fields(),
        physical.fields(),
        root(),
        session,
        &constants,
        &defaults,
    )
}

fn all_fields(fields: &Fields) -> Vec<FieldRef> {
    let mut output = Vec::new();
    for field in fields {
        output.push(field.clone());
        match field.data_type() {
            DataType::Struct(fields) => output.extend(all_fields(fields)),
            DataType::List(element)
            | DataType::LargeList(element)
            | DataType::FixedSizeList(element, _) => {
                output.extend(all_fields(&Fields::from(vec![element.clone()])))
            }
            DataType::Map(entries, _) => {
                output.extend(all_fields(&Fields::from(vec![entries.clone()])))
            }
            _ => {}
        }
    }
    output
}

fn literal(value: &Literal, dtype: &DType) -> Result<Expression> {
    Ok(lit(literal_scalar(value, dtype)?))
}

/// Materializes a missing field's default; a non-null struct default uses child defaults.
pub(super) fn initial_default(field: &NestedFieldRef, dtype: &DType) -> Result<Scalar> {
    let Some(value) = &field.initial_default else {
        return if field.required {
            Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Required field {} lacks an initial default", field.name),
            ))
        } else {
            Ok(Scalar::null(dtype.clone()))
        };
    };
    if let Literal::Struct(value) = value {
        if value.fields().iter().any(Option::is_some) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "A non-null struct initial default must be an empty object",
            ));
        }
        let (Type::Struct(children), DType::Struct(types, _)) = (field.field_type.as_ref(), dtype)
        else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Struct default has incompatible type",
            ));
        };
        let values = children
            .fields()
            .iter()
            .enumerate()
            .map(|(i, child)| {
                let dtype = types.field_by_index(i).ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Struct default arity mismatch")
                })?;
                initial_default(child, &dtype)
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Scalar::struct_(dtype.clone(), values));
    }
    literal_scalar(value, dtype)
}

fn literal_scalar(value: &Literal, dtype: &DType) -> Result<Scalar> {
    let scalar: Scalar = match value {
        Literal::Primitive(PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin) => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Out-of-domain sentinel is not an Iceberg default",
            ));
        }
        Literal::Primitive(PrimitiveLiteral::Boolean(v)) => (*v).into(),
        Literal::Primitive(PrimitiveLiteral::Int(v)) => (*v).into(),
        Literal::Primitive(PrimitiveLiteral::Long(v)) => (*v).into(),
        Literal::Primitive(PrimitiveLiteral::Float(v)) => v.0.into(),
        Literal::Primitive(PrimitiveLiteral::Double(v)) => v.0.into(),
        Literal::Primitive(PrimitiveLiteral::String(v)) => v.as_str().into(),
        Literal::Primitive(PrimitiveLiteral::Binary(v)) => {
            if let DType::FixedSizeList(element, _, n) = dtype {
                return Ok(Scalar::fixed_size_list(
                    element.as_ref().clone(),
                    v.iter().copied().map(Scalar::from).collect(),
                    *n,
                ));
            }
            v.as_slice().into()
        }
        Literal::Primitive(PrimitiveLiteral::Int128(v)) => {
            let DType::Decimal(decimal, n) = dtype else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Decimal default has incompatible type",
                ));
            };
            return Ok(Scalar::decimal(DecimalValue::I128(*v), *decimal, *n));
        }
        Literal::Primitive(PrimitiveLiteral::UInt128(v)) => {
            let storage_dtype = match dtype {
                DType::Extension(ext) => ext.storage_dtype(),
                _ => dtype,
            };
            let DType::FixedSizeList(element, 16, nullable) = storage_dtype else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "UUID default has incompatible type",
                ));
            };
            let storage = Scalar::fixed_size_list(
                element.as_ref().clone(),
                v.to_be_bytes().into_iter().map(Scalar::from).collect(),
                *nullable,
            );
            return Ok(match dtype {
                DType::Extension(ext) => Scalar::extension_ref(ext.clone(), storage),
                _ => storage,
            });
        }
        Literal::Struct(value) => {
            let DType::Struct(fields, _) = dtype else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Struct default has incompatible type",
                ));
            };
            if value.fields().len() != fields.nfields() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Struct literal arity mismatch",
                ));
            }
            let children = value
                .fields()
                .iter()
                .enumerate()
                .map(|(i, value)| {
                    let dtype = fields.field_by_index(i).ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Struct default arity mismatch")
                    })?;
                    value.as_ref().map_or_else(
                        || Ok(Scalar::null(dtype.clone())),
                        |value| literal_scalar(value, &dtype),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Scalar::struct_(dtype.clone(), children));
        }
        Literal::List(values) => {
            let DType::List(element, n) = dtype else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "List default has incompatible type",
                ));
            };
            let values = values
                .iter()
                .map(|v| {
                    v.as_ref().map_or_else(
                        || Ok(Scalar::null(element.as_ref().clone())),
                        |v| literal_scalar(v, element),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Scalar::list(element.clone(), values, *n));
        }
        Literal::Map(values) => {
            let DType::Map(map, _) = dtype else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Map default has incompatible type",
                ));
            };
            let values = values
                .clone()
                .into_iter()
                .map(|(k, v)| {
                    Ok((
                        literal_scalar(&k, &map.key_dtype())?,
                        v.as_ref().map_or_else(
                            || Ok(Scalar::null(map.value_dtype())),
                            |v| literal_scalar(v, &map.value_dtype()),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            return Scalar::try_map(dtype.clone(), values).map_err(to_iceberg_error);
        }
    };
    if let DType::Extension(ext) = dtype {
        return Ok(Scalar::extension_ref(
            ext.clone(),
            scalar.cast(ext.storage_dtype()).map_err(to_iceberg_error)?,
        ));
    }
    scalar.cast(dtype).map_err(to_iceberg_error)
}
