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

//! Parallel Parquet and Vortex scans of Iceberg snapshots in DataFusion.

mod deletes;
mod files;
mod nested;
mod schema;
#[cfg(test)]
mod tests;

use std::any::Any;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
pub(crate) use files::FileCache;
use files::OpenedFile;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::scan::FileScanTask;
use iceberg::spec::SchemaRef;
use iceberg::{Error, ErrorKind, Result};
use schema::field_id;
use vortex::array::stats::StatsSet;
use vortex::array::stream::{ArrayStreamAdapter, SendableArrayStream};
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::{DType, Field, FieldPath};
use vortex::error::{VortexError, VortexResult};
use vortex::expr::stats::{Precision, Stat};
use vortex::expr::transform::replace;
use vortex::expr::{BoundExpression, Expression, and_collect, root};
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::scalar::Scalar;
use vortex::scan::selection::Selection;
use vortex::scan::{
    DataSource, DataSourceScan, DataSourceScanRef, Partition, PartitionRef, PartitionStream,
    ScanRequest,
};
use vortex::session::VortexSession;

fn to_iceberg_error(error: VortexError) -> Error {
    Error::new(ErrorKind::Unexpected, "Vortex scan failed").with_source(error)
}

pub(crate) struct IcebergSource {
    dtype: DType,
    tasks: Vec<Arc<PreparedTask>>,
    statistics: HashMap<String, StatsSet>,
}
struct PreparedTask {
    file: Arc<OpenedFile>,
    session: VortexSession,
    normalize: Expression,
    output: Expression,
    filter: Option<Expression>,
    selection: Selection,
}
impl IcebergSource {
    /// Static filters must be safe on rows later removed by deletes; the scan
    /// converter establishes this before passing predicates to the layouts.
    pub(crate) async fn prepare(
        cache: &FileCache,
        schema: SchemaRef,
        tasks: Vec<FileScanTask>,
        filter: Option<Expression>,
        history: Vec<SchemaRef>,
    ) -> Result<Self> {
        let session = cache.session();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let dtype = session
            .arrow()
            .from_arrow_schema(&arrow_schema)
            .map_err(to_iceberg_error)?;
        let read_schema = schema::delete_read_schema(
            &schema,
            &history,
            tasks
                .iter()
                .flat_map(|task| task.deletes())
                .flat_map(|delete| delete.equality_ids().unwrap_or_default().iter().copied()),
        )?;
        let read_arrow_schema = schema_to_arrow_schema(&read_schema)?;
        let mut prepared = Vec::new();
        let mut all_stats: Vec<HashMap<String, (StatsSet, DType)>> = Vec::new();
        let opened = stream::iter(tasks.into_iter().map(|task| async move {
            let file = cache
                .open(
                    task.data_file_path(),
                    task.file_size_in_bytes(),
                    task.data_file_format(),
                )
                .await?;
            Ok::<_, Error>((task, file))
        }))
        .buffered(16)
        .try_collect::<Vec<_>>()
        .await?;
        let mut decoded_deletes = deletes::DeleteCache::new();
        for (task, file) in opened {
            if task.start() != 0
                || (task.length() != 0 && task.length() != task.file_size_in_bytes())
            {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "ColdFusion requires whole-file planning tasks before discovering natural row splits",
                ));
            }
            if task.key_metadata().is_some() {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Encrypted files require an encryption-aware reader",
                ));
            }
            let file = file.for_scan(session)?;
            let physical = schema::file_schema(file.schema.clone(), &task)?;
            let normalize = schema::normalization(&task, &read_schema, &physical, session)?;
            let output = if read_schema.as_struct() == schema.as_struct() {
                normalize.clone()
            } else {
                let projection =
                    schema::normalization(&task, &schema, &read_arrow_schema, session)?;
                replace(projection, &root(), normalize.clone())
            };
            let deletes = deletes::load(cache, &task, &read_schema, &mut decoded_deletes).await?;
            let filter = and_collect(filter.iter().cloned().chain(deletes.filters))
                .map(|e| replace(e, &root(), normalize.clone()));
            let mut statistics = HashMap::new();
            for field in arrow_schema.fields() {
                if let Some(physical) = physical
                    .fields()
                    .iter()
                    .find(|f| field_id(f) == field_id(field))
                {
                    let stats = file
                        .source
                        .field_statistics(&FieldPath::from_name(physical.name().as_str()))
                        .await
                        .map_err(to_iceberg_error)?;
                    if let Some(dtype) = file
                        .reader
                        .dtype()
                        .as_struct_fields_opt()
                        .and_then(|f| f.field(physical.name()))
                    {
                        statistics.insert(field.name().clone(), (stats, dtype));
                    }
                }
            }
            all_stats.push(statistics);
            prepared.push(Arc::new(PreparedTask {
                file,
                session: session.clone(),
                normalize,
                output,
                filter,
                selection: deletes.selection,
            }));
        }
        let mut statistics = HashMap::new();
        for field in arrow_schema.fields() {
            let target = session
                .arrow()
                .from_arrow_field(field)
                .map_err(to_iceberg_error)?;
            if !matches!(field.data_type(), DataType::Int32 | DataType::Int64) {
                continue;
            }
            let mut range: Option<(Scalar, Scalar)> = None;
            let mut complete = !all_stats.is_empty();
            for stats in &all_stats {
                let bounds = stats.get(field.name()).map(|(s, dtype)| {
                    (
                        s.get(Stat::Min).into_scalar(dtype.clone()).into_inner(),
                        s.get(Stat::Max).into_scalar(dtype.clone()).into_inner(),
                    )
                });
                let Some((Some(lo), Some(hi))) = bounds else {
                    complete = false;
                    break;
                };
                let (lo, hi) = (
                    lo.cast(&target).map_err(to_iceberg_error)?,
                    hi.cast(&target).map_err(to_iceberg_error)?,
                );
                range = Some(match range {
                    None => (lo, hi),
                    Some((min, max)) => (
                        if lo.partial_cmp(&min).is_some_and(|c| c.is_lt()) {
                            lo
                        } else {
                            min
                        },
                        if hi.partial_cmp(&max).is_some_and(|c| c.is_gt()) {
                            hi
                        } else {
                            max
                        },
                    ),
                });
            }
            if complete && let Some((lo, hi)) = range {
                let mut stats = StatsSet::default();
                stats.set(Stat::Min, Precision::Inexact(lo.into_value().unwrap()));
                stats.set(Stat::Max, Precision::Inexact(hi.into_value().unwrap()));
                statistics.insert(field.name().clone(), stats);
            }
        }
        Ok(Self {
            dtype,
            tasks: prepared,
            statistics,
        })
    }
}
#[async_trait]
impl DataSource for IcebergSource {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
    fn row_count(&self) -> Precision<u64> {
        let Some(rows) = self.tasks.iter().try_fold(0_u64, |rows, task| {
            rows.checked_add(task.file.reader.row_count())
        }) else {
            return Precision::Absent;
        };
        if self
            .tasks
            .iter()
            .any(|t| t.filter.is_some() || !matches!(t.selection, Selection::All))
        {
            Precision::Inexact(rows)
        } else {
            Precision::Exact(rows)
        }
    }
    fn byte_size(&self) -> Precision<u64> {
        Precision::Absent
    }
    async fn field_statistics(&self, field: &FieldPath) -> VortexResult<StatsSet> {
        let [Field::Name(name)] = field.parts() else {
            return Ok(StatsSet::default());
        };
        Ok(self
            .statistics
            .get(name.as_ref())
            .cloned()
            .unwrap_or_default())
    }
    async fn scan(&self, request: ScanRequest) -> VortexResult<DataSourceScanRef> {
        let dtype = request
            .projection
            .clone()
            .bind(&self.dtype)?
            .dtype()
            .clone();
        let remaining = request
            .limit
            .filter(|_| !request.ordered)
            .map(|n| Arc::new(AtomicU64::new(n)));
        let mut partitions = Vec::new();
        let mut index = 0usize;
        for task in &self.tasks {
            let projection = replace(request.projection.clone(), &root(), task.output.clone())
                .bind(task.file.reader.dtype())?
                .optimize_recursive()?;
            let filter = and_collect(
                task.filter.iter().cloned().chain(
                    request
                        .filter
                        .iter()
                        .map(|e| replace(e.clone(), &root(), task.normalize.clone())),
                ),
            )
            .map(|e| e.bind(task.file.reader.dtype())?.optimize_recursive())
            .transpose()?;
            let scan = ScanBuilder::new(task.session.clone(), task.file.reader.clone())
                .with_projection(projection.clone())
                .with_some_filter(filter.clone())
                .with_split_by(task.file.scan_splits());
            let boundaries = scan.full_file_splits()?;
            for rows in boundaries.windows(2).map(|w| w[0]..w[1]) {
                let accepted = request
                    .partition_range
                    .as_ref()
                    .is_none_or(|r| r.contains(&(index as u64)))
                    && !request
                        .partition_selection
                        .row_mask(&(index as u64..index as u64 + 1))
                        .mask()
                        .all_false();
                if accepted {
                    let selection = task_selection(&request.selection, &rows);
                    let selection = match (&selection, &task.selection) {
                        (_, Selection::All) => selection,
                        (Selection::All, _) => task.selection.clone(),
                        _ => {
                            let left = selection.row_mask(&rows);
                            let right = task.selection.row_mask(&rows);
                            Selection::IncludeRoaring(
                                (rows.start..rows.end)
                                    .filter(|row| {
                                        left.mask().value((*row - rows.start) as usize)
                                            && right.mask().value((*row - rows.start) as usize)
                                    })
                                    .collect(),
                            )
                        }
                    };
                    partitions.push(Box::new(TaskPartition {
                        index,
                        task: task.clone(),
                        rows: task_rows(&rows, request.row_range.as_ref()),
                        selection,
                        projection: projection.clone(),
                        filter: filter.clone(),
                        ordered: request.ordered,
                        limit: request.limit,
                        remaining: remaining.clone(),
                    }) as PartitionRef);
                }
                index += 1;
            }
        }
        Ok(Box::new(TaskScan { dtype, partitions }))
    }
}
struct TaskScan {
    dtype: DType,
    partitions: Vec<PartitionRef>,
}

impl DataSourceScan for TaskScan {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn partition_count(&self) -> Precision<usize> {
        Precision::Exact(self.partitions.len())
    }

    fn partitions(self: Box<Self>) -> PartitionStream {
        stream::iter(self.partitions.into_iter().map(Ok)).boxed()
    }
}

struct TaskPartition {
    index: usize,
    task: Arc<PreparedTask>,
    rows: Range<u64>,
    selection: Selection,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    ordered: bool,
    limit: Option<u64>,
    remaining: Option<Arc<AtomicU64>>,
}

impl Partition for TaskPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn index(&self) -> usize {
        self.index
    }

    fn row_count(&self) -> Precision<u64> {
        let rows = self.rows.end - self.rows.start;
        if self.task.filter.is_some()
            || self.filter.is_some()
            || !matches!(self.selection, Selection::All)
            || self.limit.is_some()
        {
            Precision::Inexact(rows)
        } else {
            Precision::Exact(rows)
        }
    }

    fn byte_size(&self) -> Precision<u64> {
        Precision::Absent
    }

    fn execute(self: Box<Self>) -> VortexResult<SendableArrayStream> {
        let dtype = self.projection.dtype().clone();
        let mut scan = ScanBuilder::new(self.task.session.clone(), self.task.file.reader.clone())
            .with_projection(self.projection)
            .with_some_filter(self.filter)
            .with_row_range(self.rows)
            .with_selection(self.selection)
            .with_ordered(self.ordered)
            .with_split_by(self.task.file.scan_splits());
        if let Some(limit) = self.limit {
            scan = scan.with_limit(limit);
        }
        let mut arrays = Box::pin(scan.into_array_stream()?);
        let Some(remaining) = self.remaining else {
            return Ok(arrays);
        };
        let limited = stream::poll_fn(move |cx| {
            if remaining.load(Ordering::Acquire) == 0 {
                return Poll::Ready(None);
            }
            match arrays.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(array))) => {
                    if array.is_empty() {
                        return Poll::Ready(Some(Ok(array)));
                    }
                    let granted = reserve_rows(&remaining, array.len() as u64) as usize;
                    if granted == 0 {
                        Poll::Ready(None)
                    } else if granted < array.len() {
                        Poll::Ready(Some(array.slice(0..granted)))
                    } else {
                        Poll::Ready(Some(Ok(array)))
                    }
                }
                other => other,
            }
        });
        Ok(Box::pin(ArrayStreamAdapter::new(dtype, limited)))
    }
}

fn reserve_rows(remaining: &AtomicU64, requested: u64) -> u64 {
    let mut rows = remaining.load(Ordering::Acquire);
    loop {
        let granted = rows.min(requested);
        match remaining.compare_exchange_weak(
            rows,
            rows - granted,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return granted,
            Err(current) => rows = current,
        }
    }
}

fn task_rows(rows: &Range<u64>, requested: Option<&Range<u64>>) -> Range<u64> {
    match requested {
        Some(requested) => {
            rows.start.saturating_add(requested.start).min(rows.end)
                ..rows.start.saturating_add(requested.end).min(rows.end)
        }
        None => rows.clone(),
    }
}

fn task_selection(selection: &Selection, rows: &Range<u64>) -> Selection {
    let (include, indices): (bool, Box<dyn Iterator<Item = u64> + '_>) = match selection {
        Selection::All => return Selection::All,
        Selection::IncludeByIndex(indices) => (true, Box::new(indices.iter().copied())),
        Selection::ExcludeByIndex(indices) => (false, Box::new(indices.iter().copied())),
        Selection::IncludeRoaring(indices) => (true, Box::new(indices.iter())),
        Selection::ExcludeRoaring(indices) => (false, Box::new(indices.iter())),
    };
    let selected = indices
        .take_while(|index| *index < rows.end - rows.start)
        .map(|index| rows.start + index)
        .collect();
    if include {
        Selection::IncludeRoaring(selected)
    } else {
        Selection::ExcludeRoaring(selected)
    }
}
