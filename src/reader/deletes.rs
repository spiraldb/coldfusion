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

//! Snapshot delete files evaluated inside Vortex scans.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_row::{RowConverter, SortField};
use datafusion::arrow::array::{
    Array, ArrayRef as ArrowArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array,
    StringArray, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, Schema};
use iceberg::{Error, ErrorKind, Result};
use vortex::array::{ArrayRef, ExecutionCtx, VortexSessionExecute};
use vortex::arrow::{ArrowSessionExt, ExecuteArrowExt};
use vortex::dtype::{DType, Nullability};
use vortex::error::VortexResult;
use vortex::expr::{Expression, get_item, root};
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::scalar_fn::{
    Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable, ScalarFnVTableExt,
};
use vortex::scan::selection::Selection;

use super::files::FileCache;
use super::schema::{field_id, field_path, file_schema};
use super::to_iceberg_error;

pub(super) struct Deletes {
    pub selection: Selection,
    pub filters: Vec<Expression>,
}

#[derive(Clone)]
pub(super) struct LoadedDelete {
    equality: Option<Expression>,
    positions: HashMap<String, Vec<u64>>,
}
pub(super) type DeleteCache = HashMap<String, LoadedDelete>;

pub(super) async fn load(
    cache: &FileCache,
    task: &FileScanTask,
    read_schema: &Schema,
    decoded: &mut DeleteCache,
) -> Result<Deletes> {
    let session = cache.session();
    let logical = schema_to_arrow_schema(read_schema)?;
    let mut positions = Vec::new();
    let mut filters = Vec::new();
    for delete in task.deletes() {
        if delete.key_metadata().is_some() {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encrypted delete files require an encryption-aware reader",
            ));
        }
        if let Some(loaded) = decoded.get(delete.file_path()) {
            positions.extend(
                loaded
                    .positions
                    .get(task.data_file_path())
                    .into_iter()
                    .flatten()
                    .copied(),
            );
            filters.extend(loaded.equality.iter().cloned());
            continue;
        }
        let mut loaded = LoadedDelete {
            equality: None,
            positions: HashMap::new(),
        };
        let opened = cache
            .open(
                delete.file_path(),
                delete.file_size_in_bytes(),
                delete.file_format(),
            )
            .await?;
        let opened = opened.for_scan(session)?;
        let mut batches = ScanBuilder::new(session.clone(), opened.reader.clone())
            .into_array_stream()
            .map_err(to_iceberg_error)?
            .map_err(to_iceberg_error);

        let physical = file_schema(opened.schema.clone(), task)?;
        let paths = delete
            .equality_ids()
            .unwrap_or_default()
            .iter()
            .map(|id| {
                field_path(logical.fields(), *id).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Equality-delete key {id} is missing"),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = paths
            .iter()
            .map(|path| {
                Arc::new(
                    path.last()
                        .unwrap()
                        .as_ref()
                        .clone()
                        .with_nullable(path.iter().any(|field| field.is_nullable())),
                )
            })
            .collect::<Vec<_>>();
        let converter = RowConverter::new(
            keys.iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;
        let mut rows = HashSet::new();
        while let Some(array) = batches.try_next().await? {
            let batch = array
                .execute_arrow(None, &mut session.create_execution_ctx())
                .map_err(to_iceberg_error)?;
            let batch =
                RecordBatch::from(batch.as_any().downcast_ref::<StructArray>().ok_or_else(
                    || Error::new(ErrorKind::DataInvalid, "Delete file is not a struct"),
                )?);
            match delete.file_type() {
                DataContentType::PositionDeletes => {
                    let paths = batch.column_by_name("file_path").ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Position deletes lack file_path")
                    })?;
                    let paths = cast(paths, &DataType::Utf8)?;
                    let paths = paths.as_any().downcast_ref::<StringArray>().unwrap();
                    let ids = batch
                        .column_by_name("pos")
                        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                        .ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "Position deletes lack pos")
                        })?;
                    for (path, pos) in paths.iter().zip(ids.iter()) {
                        if let Some(path) = path {
                            let pos = pos.and_then(|n| u64::try_from(n).ok()).ok_or_else(|| {
                                Error::new(ErrorKind::DataInvalid, "Invalid positional delete")
                            })?;
                            loaded
                                .positions
                                .entry(path.to_owned())
                                .or_default()
                                .push(pos);
                        }
                    }
                }
                DataContentType::EqualityDeletes => {
                    let columns = keys
                        .iter()
                        .map(|key| {
                            let path = field_path(physical.fields(), field_id(key).unwrap())
                                .ok_or_else(|| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        "Equality-delete key is missing",
                                    )
                                })?;
                            let mut array =
                                Arc::new(StructArray::from(batch.clone())) as ArrowArrayRef;
                            for field in path {
                                let parent = array
                                    .as_any()
                                    .downcast_ref::<StructArray>()
                                    .ok_or_else(|| {
                                        Error::new(
                                            ErrorKind::DataInvalid,
                                            "Equality-delete key parent is not a struct",
                                        )
                                    })?;
                                let index = parent
                                    .fields()
                                    .iter()
                                    .position(|f| f.name() == field.name())
                                    .ok_or_else(|| {
                                        Error::new(
                                            ErrorKind::DataInvalid,
                                            "Equality-delete key is missing from decoded array",
                                        )
                                    })?;
                                array = parent.flatten().1.swap_remove(index);
                            }
                            Ok(cast(&array, key.data_type())?)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let encoded = converter.convert_columns(&canonical_keys(columns))?;
                    rows.extend(encoded.iter().map(|row| row.as_ref().to_vec()));
                }
                _ => return Err(Error::new(ErrorKind::DataInvalid, "Not a delete file")),
            }
        }
        if delete.file_type() == DataContentType::EqualityDeletes && !rows.is_empty() {
            let children = paths
                .iter()
                .map(|path| {
                    path.iter().fold(root(), |input, field| {
                        get_item(field.name().as_str(), input)
                    })
                })
                .collect::<Vec<_>>();
            let options = DeleteKeys(Arc::new(KeySet {
                fields: keys,
                rows,
                converter,
            }));
            loaded.equality = Some(
                Expression::try_new(KeepUndeleted.bind(options), children)
                    .map_err(to_iceberg_error)?,
            );
        }
        positions.extend(
            loaded
                .positions
                .get(task.data_file_path())
                .into_iter()
                .flatten()
                .copied(),
        );
        filters.extend(loaded.equality.iter().cloned());
        decoded.insert(delete.file_path().to_owned(), loaded);
    }
    positions.sort_unstable();
    positions.dedup();
    Ok(Deletes {
        selection: if positions.is_empty() {
            Selection::All
        } else {
            Selection::ExcludeRoaring(positions.into_iter().collect())
        },
        filters,
    })
}

fn canonical_keys(columns: Vec<ArrowArrayRef>) -> Vec<ArrowArrayRef> {
    columns
        .into_iter()
        .map(|array| {
            if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
                Arc::new(Float32Array::from_iter(values.iter().map(|v| {
                    v.map(|v| {
                        if v.is_nan() {
                            f32::NAN
                        } else if v == 0.0 {
                            0.0
                        } else {
                            v
                        }
                    })
                }))) as ArrowArrayRef
            } else if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                Arc::new(Float64Array::from_iter(values.iter().map(|v| {
                    v.map(|v| {
                        if v.is_nan() {
                            f64::NAN
                        } else if v == 0.0 {
                            0.0
                        } else {
                            v
                        }
                    })
                }))) as ArrowArrayRef
            } else {
                array
            }
        })
        .collect()
}
struct KeySet {
    fields: Vec<FieldRef>,
    rows: HashSet<Vec<u8>>,
    converter: RowConverter,
}
#[derive(Clone)]
struct DeleteKeys(Arc<KeySet>);
impl PartialEq for DeleteKeys {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for DeleteKeys {}
impl Hash for DeleteKeys {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}
impl fmt::Display for DeleteKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} delete keys", self.0.rows.len())
    }
}
impl fmt::Debug for DeleteKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
#[derive(Clone)]
struct KeepUndeleted;
impl ScalarFnVTable for KeepUndeleted {
    type Options = DeleteKeys;
    fn id(&self) -> ScalarFnId {
        ScalarFnId::new_static("coldfusion.keep_undeleted")
    }
    fn arity(&self, o: &DeleteKeys) -> Arity {
        Arity::Exact(o.0.fields.len())
    }
    fn child_name(&self, _: &DeleteKeys, index: usize) -> ChildName {
        Arc::<str>::from(format!("key{index}")).into()
    }
    fn return_dtype(&self, _: &DeleteKeys, _: &[DType]) -> VortexResult<DType> {
        Ok(DType::Bool(Nullability::NonNullable))
    }
    fn execute(
        &self,
        o: &DeleteKeys,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let session = ctx.session().clone();
        let columns =
            o.0.fields
                .iter()
                .enumerate()
                .map(|(i, field)| args.get(i)?.execute_arrow(Some(field), ctx))
                .collect::<VortexResult<Vec<_>>>()?;
        let encoded = o.0.converter.convert_columns(&canonical_keys(columns))?;
        let keep = BooleanArray::from(
            encoded
                .iter()
                .map(|row| !o.0.rows.contains(row.as_ref()))
                .collect::<Vec<_>>(),
        );
        session.arrow().from_arrow_array(
            Arc::new(keep),
            &Field::new("keep", DataType::Boolean, false),
        )
    }
}
