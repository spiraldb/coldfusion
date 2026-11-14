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

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef as ArrowArrayRef, LargeListArray, ListArray, MapArray, StructArray,
    new_null_array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use iceberg::Result;
use iceberg::spec::NestedFieldRef;
use vortex::array::arrays::ConstantArray;
use vortex::array::{ArrayRef, ExecutionCtx, IntoArray};
use vortex::arrow::{ArrowSessionExt, ExecuteArrowExt};
use vortex::dtype::DType;
use vortex::error::{VortexResult, vortex_err};
use vortex::expr::Expression;
use vortex::scalar_fn::{
    Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable, ScalarFnVTableExt,
};
use vortex::session::VortexSession;

use super::schema::{field_id, initial_default};
use super::to_iceberg_error;

pub(super) fn transform(
    input: Expression,
    source: FieldRef,
    target: FieldRef,
    defaults: Arc<HashMap<i32, NestedFieldRef>>,
    session: &VortexSession,
) -> Result<Expression> {
    let dtype = session
        .arrow()
        .from_arrow_field(&target)
        .map_err(to_iceberg_error)?;
    Expression::try_new(
        NestedFieldTransform.bind(Options(Arc::new(Fields {
            source,
            target,
            defaults,
            dtype,
        }))),
        [input],
    )
    .map_err(to_iceberg_error)
}
struct Fields {
    source: FieldRef,
    target: FieldRef,
    defaults: Arc<HashMap<i32, NestedFieldRef>>,
    dtype: DType,
}
#[derive(Clone)]
struct Options(Arc<Fields>);
impl PartialEq for Options {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Options {}
impl Hash for Options {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IcebergNestedFields")
            .field(&self.0.target)
            .finish()
    }
}
impl fmt::Display for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.target.name())
    }
}
#[derive(Clone)]
struct NestedFieldTransform;
impl ScalarFnVTable for NestedFieldTransform {
    type Options = Options;
    fn id(&self) -> ScalarFnId {
        ScalarFnId::new_static("coldfusion.nested_fields")
    }
    fn arity(&self, _: &Options) -> Arity {
        Arity::Exact(1)
    }
    fn child_name(&self, _: &Options, _: usize) -> ChildName {
        "value".into()
    }
    fn return_dtype(&self, o: &Options, _: &[DType]) -> VortexResult<DType> {
        Ok(o.0.dtype.clone())
    }
    fn execute(
        &self,
        o: &Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let session = ctx.session().clone();
        let array = args.get(0)?.execute_arrow(Some(&o.0.source), ctx)?;
        let result = adapt(&array, &o.0.source, &o.0.target, &o.0.defaults, ctx)?;
        session.arrow().from_arrow_array(result, &o.0.target)
    }
}

fn missing(
    field: &FieldRef,
    len: usize,
    defaults: &HashMap<i32, NestedFieldRef>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    if let Some(default) = field_id(field).and_then(|id| defaults.get(&id)) {
        let dtype = ctx.session().arrow().from_arrow_field(field)?;
        let default = initial_default(default, &dtype).map_err(|error| vortex_err!("{error}"))?;
        return ConstantArray::new(default, len)
            .into_array()
            .execute_arrow(Some(field), ctx);
    }
    if !field.is_nullable() {
        return Err(vortex_err!(
            "Required nested Iceberg field {} is missing",
            field.name()
        ));
    }
    Ok(new_null_array(field.data_type(), len))
}

fn adapt(
    array: &ArrowArrayRef,
    source: &FieldRef,
    target: &FieldRef,
    defaults: &HashMap<i32, NestedFieldRef>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrowArrayRef> {
    match (source.data_type(), target.data_type()) {
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
            let input = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| vortex_err!("Expected struct"))?;
            let columns = target_fields
                .iter()
                .map(|target| {
                    match source_fields.iter().position(|source| {
                        field_id(source).is_some() && field_id(source) == field_id(target)
                    }) {
                        Some(i) => adapt(input.column(i), &source_fields[i], target, defaults, ctx),
                        None => missing(target, array.len(), defaults, ctx),
                    }
                })
                .collect::<VortexResult<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                target_fields.clone(),
                columns,
                input.nulls().cloned(),
            )?))
        }
        (DataType::List(source_element), DataType::List(target_element)) => {
            let input = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| vortex_err!("Expected list"))?;
            let values = adapt(
                input.values(),
                source_element,
                target_element,
                defaults,
                ctx,
            )?;
            Ok(Arc::new(ListArray::try_new(
                target_element.clone(),
                input.offsets().clone(),
                values,
                input.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(source_element), DataType::List(target_element)) => {
            let input = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| vortex_err!("Expected large list"))?;
            let values = adapt(
                input.values(),
                source_element,
                target_element,
                defaults,
                ctx,
            )?;
            let list: ArrowArrayRef = Arc::new(LargeListArray::try_new(
                target_element.clone(),
                input.offsets().clone(),
                values,
                input.nulls().cloned(),
            )?);
            Ok(cast(&list, target.data_type())?)
        }
        (DataType::Map(source_entries, _), DataType::Map(target_entries, ordered)) => {
            let input = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| vortex_err!("Expected map"))?;
            let entries: ArrowArrayRef = Arc::new(input.entries().clone());
            let entries = adapt(&entries, source_entries, target_entries, defaults, ctx)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| vortex_err!("Expected map entries"))?;
            Ok(Arc::new(MapArray::try_new(
                target_entries.clone(),
                input.offsets().clone(),
                entries.clone(),
                input.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => Ok(cast(array, target.data_type())?),
    }
}
