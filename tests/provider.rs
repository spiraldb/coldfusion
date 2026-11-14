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
use std::sync::Arc;

use coldfusion::{IcebergTableProvider, iceberg};
use datafusion::arrow::array::{
    ArrayRef, BinaryViewArray, BooleanArray, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, StringArray, StringViewArray, StructArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::common::stats::Precision;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal as PhysicalLiteral,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
};
use datafusion::physical_plan::statistics::StatisticsArgs;
use datafusion::physical_plan::{
    ExecutionPlanProperties, collect, collect_partitioned, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
    ManifestListWriter, ManifestWriterBuilder, NestedField, Operation, PrimitiveType, Snapshot,
    Struct, Summary, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::{
    FileWriter, FileWriterBuilder, ParquetWriterBuilder, VortexWriterBuilder,
};
use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, Runtime, TableCreation};
use iceberg_vortex::VortexSessionDefault as _;
use iceberg_vortex::session::VortexSession as IcebergVortexSession;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::TempDir;

const ROWS: i64 = 257;

fn with_dynamic_filter(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    filter: Arc<dyn PhysicalExpr>,
    context: &SessionContext,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    let result = plan
        .handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            ChildPushdownResult {
                parent_filters: vec![ChildFilterPushdownResult {
                    filter,
                    child_results: vec![],
                }],
                self_filters: vec![],
            },
            context.state().config_options(),
        )
        .unwrap();
    assert!(matches!(result.filters.as_slice(), [PushedDown::No]));
    result.updated_node.unwrap()
}

struct Fixture {
    _root: TempDir,
    catalog: MemoryCatalog,
    table: Table,
    batch: RecordBatch,
    data_path: String,
}

impl Fixture {
    async fn new(write: bool) -> Self {
        Self::new_with_id_type(write, DataType::Int64).await
    }

    async fn new_with_id_type(write: bool, id_type: DataType) -> Self {
        Self::new_with_id_type_and_format(write, id_type, DataFileFormat::Parquet).await
    }

    async fn new_with_format(write: bool, format: DataFileFormat) -> Self {
        Self::new_with_id_type_and_format(write, DataType::Int64, format).await
    }

    async fn new_with_id_type_and_format(
        write: bool,
        id_type: DataType,
        format: DataFileFormat,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .with_runtime(Runtime::current())
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    format!("file://{}", root.path().display()),
                )]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("test".to_owned());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = arrow_schema_to_schema_auto_assign_ids(&Schema::new(vec![
            Field::new("id", id_type.clone(), id_type == DataType::Int32),
            Field::new("label", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new(
                "payload",
                DataType::Struct(
                    vec![
                        Field::new("code", DataType::Int64, true),
                        Field::new("text", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("special_float", DataType::Float64, true),
            Field::new("blob", DataType::Binary, true),
        ]))
        .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".to_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();
        let arrow_schema =
            Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
        let DataType::Struct(payload_fields) = arrow_schema.field(3).data_type() else {
            unreachable!()
        };
        let ids: ArrayRef = match id_type {
            DataType::Int32 => Arc::new(Int32Array::from_iter(
                (0..ROWS).map(|id| (id % 13 != 0).then_some(id as i32)),
            )),
            DataType::Int64 => Arc::new(Int64Array::from_iter_values(0..ROWS)),
            _ => unreachable!(),
        };
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            ids,
            Arc::new(StringArray::from_iter(
                (0..ROWS).map(|id| (id % 11 != 0).then(|| format!("row-{id}"))),
            )),
            Arc::new(BooleanArray::from_iter(
                (0..ROWS).map(|id| (id % 7 != 0).then_some(id % 2 == 0)),
            )),
            Arc::new(StructArray::new(
                payload_fields.clone(),
                vec![
                    Arc::new(Int64Array::from_iter(
                        (0..ROWS).map(|id| (id % 5 != 0).then_some(id * 3)),
                    )),
                    Arc::new(StringArray::from_iter(
                        (0..ROWS).map(|id| (id % 5 != 0).then(|| format!("nested-{id}"))),
                    )),
                ],
                Some(NullBuffer::from_iter((0..ROWS).map(|id| id % 5 != 0))),
            )),
            Arc::new(Float64Array::from(vec![f64::NAN; ROWS as usize])),
            Arc::new(LargeBinaryArray::from_iter(
                (0..ROWS).map(|id| (id % 11 != 0).then(|| id.to_le_bytes().to_vec())),
            )),
        ])
        .unwrap();
        let extension = match format {
            DataFileFormat::Parquet => "parquet",
            DataFileFormat::Vortex => "vortex",
            _ => unreachable!(),
        };
        let data_path = format!("{}/rows.{extension}", table.metadata().location());
        let mut fixture = Self {
            _root: root,
            catalog,
            table,
            batch,
            data_path,
        };
        if write {
            let path = fixture.data_path.clone();
            let batch = fixture.batch.clone();
            fixture.write_batch(&path, format, &batch).await;
        }
        fixture
    }

    async fn write_batch(&mut self, path: &str, format: DataFileFormat, batch: &RecordBatch) {
        let output = self.table.file_io().new_output(path).unwrap();
        let schema = self.table.metadata().current_schema().clone();
        let files = match format {
            DataFileFormat::Parquet => {
                let mut writer = ParquetWriterBuilder::new(
                    WriterProperties::builder()
                        .set_max_row_group_row_count(Some(32))
                        .build(),
                    schema,
                )
                .build(output)
                .await
                .unwrap();
                writer.write(batch).await.unwrap();
                writer.close().await.unwrap()
            }
            DataFileFormat::Vortex => {
                let mut writer = VortexWriterBuilder::new(schema, IcebergVortexSession::default())
                    .build(output)
                    .await
                    .unwrap();
                writer.write(batch).await.unwrap();
                writer.close().await.unwrap()
            }
            _ => unreachable!(),
        };
        self.append_files(
            files
                .into_iter()
                .map(|mut file| {
                    file.content(DataContentType::Data)
                        .partition(Struct::empty())
                        .partition_spec_id(0)
                        .build()
                        .unwrap()
                })
                .collect(),
        )
        .await;
    }

    async fn append_files(&mut self, files: Vec<DataFile>) {
        let tx = Transaction::new(&self.table);
        self.table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .unwrap()
            .commit(&self.catalog)
            .await
            .unwrap();
    }

    fn context(&self) -> SessionContext {
        SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(4)
                .with_batch_size(19),
        )
    }

    fn evolve_field(&mut self, index: usize, update: impl FnOnce(&mut NestedField)) {
        let current = self.table.metadata().current_schema();
        let mut fields = current.as_struct().fields().to_vec();
        let mut field = fields[index].as_ref().clone();
        update(&mut field);
        fields[index] = Arc::new(field);
        self.replace_schema_fields(fields);
    }

    fn replace_schema_fields(&mut self, fields: Vec<Arc<NestedField>>) {
        let current = self.table.metadata().current_schema();
        let schema_id = current.schema_id() + 1;
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(schema_id)
            .with_fields(fields)
            .build()
            .unwrap();
        let metadata = self
            .table
            .metadata()
            .clone()
            .into_builder(self.table.metadata_location().map(str::to_owned))
            .add_schema(schema)
            .unwrap()
            .set_current_schema(schema_id)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        self.table = Table::builder()
            .metadata(metadata)
            .identifier(self.table.identifier().clone())
            .file_io(self.table.file_io().clone())
            .runtime(Runtime::current())
            .build()
            .unwrap();
    }

    async fn add_deletes(&mut self) {
        let table = &self.table;
        let positions = [0, 32, 128, 256];
        let pos_schema =
            Arc::new(Schema::new(vec![
                Field::new(RESERVED_COL_NAME_DELETE_FILE_PATH, DataType::Utf8, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_owned(),
                        RESERVED_FIELD_ID_DELETE_FILE_PATH.to_string(),
                    )])),
                Field::new(RESERVED_COL_NAME_DELETE_FILE_POS, DataType::Int64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_owned(),
                        RESERVED_FIELD_ID_DELETE_FILE_POS.to_string(),
                    )])),
            ]));
        let pos_batch = RecordBatch::try_new(pos_schema, vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                self.data_path.clone(),
                positions.len(),
            ))),
            Arc::new(Int64Array::from_iter_values(positions)),
        ])
        .unwrap();
        let eq_batch =
            RecordBatch::try_new(Arc::new(self.batch.schema().project(&[0]).unwrap()), vec![
                Arc::new(Int64Array::from(vec![31, 97, 255])),
            ])
            .unwrap();
        let parent = table.metadata().current_snapshot().unwrap();
        let sequence = table.metadata().next_sequence_number();
        let snapshot_id = parent.snapshot_id() + 1;
        let mut delete_manifest = ManifestWriterBuilder::new(
            table
                .file_io()
                .new_output(format!(
                    "{}/delete-manifest.avro",
                    table.metadata().location()
                ))
                .unwrap(),
            Some(snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_deletes();
        for (name, batch, content, equality_ids) in [
            (
                "position",
                pos_batch,
                DataContentType::PositionDeletes,
                None,
            ),
            (
                "equality",
                eq_batch,
                DataContentType::EqualityDeletes,
                Some(vec![
                    table
                        .metadata()
                        .current_schema()
                        .field_by_name("id")
                        .unwrap()
                        .id,
                ]),
            ),
        ] {
            let path = self._root.path().join(format!("{name}-delete.parquet"));
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let file = DataFileBuilder::default()
                .content(content)
                .file_path(path.to_string_lossy().into_owned())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
                .record_count(batch.num_rows() as u64)
                .partition_spec_id(0)
                .partition(Struct::empty())
                .equality_ids(equality_ids)
                .build()
                .unwrap();
            delete_manifest.add_file(file, sequence).unwrap();
        }
        let mut manifests = table
            .manifest_list_reader(parent)
            .load()
            .await
            .unwrap()
            .entries()
            .to_vec();
        manifests.push(delete_manifest.write_manifest_file().await.unwrap());
        let manifest_list = format!("{}/delete-manifest-list.avro", table.metadata().location());
        let mut writer = ManifestListWriter::v2(
            table
                .file_io()
                .new_output(&manifest_list)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            snapshot_id,
            Some(parent.snapshot_id()),
            sequence,
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(Some(parent.snapshot_id()))
            .with_sequence_number(sequence)
            .with_timestamp_ms(parent.timestamp_ms() + 1)
            .with_manifest_list(manifest_list)
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(table.metadata().current_schema_id())
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(table.metadata_location().map(str::to_owned))
            .set_branch_snapshot(snapshot, "main")
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        self.table = Table::builder()
            .metadata(metadata)
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .runtime(Runtime::current())
            .build()
            .unwrap();
    }
}

#[test]
fn parallel_single_file_scans_preserve_projection_and_row_counts() {
    let query_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cpu_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Runtime::new_with_split(&query_runtime, &cpu_runtime);
    let (_fixture, plan, context) = query_runtime.block_on(async {
        let fixture = Fixture::new(true).await;
        let provider =
            IcebergTableProvider::try_new(fixture.table.clone().with_runtime(runtime)).unwrap();
        let context = fixture.context();
        let state = context.state();
        assert_eq!(provider.schema().field(1).data_type(), &DataType::Utf8View);
        assert_eq!(
            provider.schema().field(5).data_type(),
            &DataType::BinaryView
        );
        let DataType::Struct(payload) = provider.schema().field(3).data_type().clone() else {
            unreachable!()
        };
        assert_eq!(payload[1].data_type(), &DataType::Utf8View);
        let projection = vec![3, 1, 0, 5];
        let plan = provider
            .scan(&state, Some(&projection), &[], None)
            .await
            .unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 4);
        let partitions = collect_partitioned(plan, context.task_ctx()).await.unwrap();
        assert!(
            partitions
                .iter()
                .filter(|batches| !batches.is_empty())
                .count()
                > 1
        );
        let mut ids = Vec::new();
        let expected_schema = Arc::new(provider.schema().project(&projection).unwrap());
        for batch in partitions.into_iter().flatten() {
            assert_eq!(batch.schema(), expected_schema);
            assert_eq!(batch.schema().field(0).name(), "payload");
            assert_eq!(batch.schema().field(1).name(), "label");
            assert_eq!(batch.column(1).data_type(), &DataType::Utf8View);
            assert_eq!(batch.column(3).data_type(), &DataType::BinaryView);
            let column = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let blobs = batch
                .column(3)
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap();
            assert_eq!(
                blobs
                    .iter()
                    .map(|value| value.map(<[u8]>::to_vec))
                    .collect::<Vec<_>>(),
                column
                    .values()
                    .iter()
                    .map(|id| (id % 11 != 0).then(|| id.to_le_bytes().to_vec()))
                    .collect::<Vec<_>>()
            );
            ids.extend(column.values().iter().copied());
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..ROWS).collect::<Vec<_>>());

        let empty = vec![];
        let plan = provider
            .scan(&state, Some(&empty), &[], None)
            .await
            .unwrap();
        let batches = collect(plan, context.task_ctx()).await.unwrap();
        assert_eq!(
            (
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                batches.iter().all(|b| b.num_columns() == 0)
            ),
            (ROWS as usize, true)
        );
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        (fixture, plan, context.task_ctx())
    });
    cpu_runtime.shutdown_background();
    let batches = query_runtime.block_on(collect(plan, context)).unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize
    );
}

#[tokio::test]
async fn parquet_page_indexes_preserve_unprojected_filters_and_deletes() {
    const ROW_GROUP_ROWS: usize = 8192;
    const TOTAL_ROWS: usize = ROW_GROUP_ROWS * 2;
    for write_indexes in [true, false] {
        let mut fixture = Fixture::new(false).await;
        let base = datafusion::arrow::compute::concat_batches(
            &fixture.batch.schema(),
            std::iter::repeat_n(&fixture.batch, TOTAL_ROWS.div_ceil(ROWS as usize)),
        )
        .unwrap()
        .slice(0, TOTAL_ROWS);
        let label = |row| format!("row-{row:05}-{}", "x".repeat(256));
        let mut columns = base.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from_iter_values(
            (0..TOTAL_ROWS).map(|row| (row % ROW_GROUP_ROWS) as i64),
        ));
        columns[1] = Arc::new(StringArray::from_iter_values((0..TOTAL_ROWS).map(label)));
        let batch = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .set_data_page_row_count_limit(128)
            .set_data_page_size_limit(4096)
            .set_write_batch_size(128)
            .set_statistics_enabled(if write_indexes {
                EnabledStatistics::Page
            } else {
                EnabledStatistics::Chunk
            })
            .set_offset_index_disabled(!write_indexes)
            .build();
        let mut writer = ParquetWriterBuilder::new(
            properties,
            fixture.table.metadata().current_schema().clone(),
        )
        .build(
            fixture
                .table
                .file_io()
                .new_output(&fixture.data_path)
                .unwrap(),
        )
        .await
        .unwrap();
        writer.write(&batch).await.unwrap();
        let files = writer
            .close()
            .await
            .unwrap()
            .into_iter()
            .map(|mut file| {
                file.content(DataContentType::Data)
                    .partition(Struct::empty())
                    .partition_spec_id(0)
                    .build()
                    .unwrap()
            })
            .collect();
        fixture.append_files(files).await;
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(
                fixture
                    .data_path
                    .strip_prefix("file://")
                    .unwrap_or(&fixture.data_path),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
        for column in reader
            .metadata()
            .row_groups()
            .iter()
            .flat_map(|group| group.columns().iter().take(2))
        {
            assert_eq!(column.column_index_offset().is_some(), write_indexes);
            assert_eq!(column.offset_index_offset().is_some(), write_indexes);
        }
        assert!(
            std::fs::metadata(
                fixture
                    .data_path
                    .strip_prefix("file://")
                    .unwrap_or(&fixture.data_path)
            )
            .unwrap()
            .len()
                > 2 * 1024 * 1024
        );
        fixture.add_deletes().await;
        let provider = IcebergTableProvider::try_new(fixture.table.clone()).unwrap();
        let expected = (0..TOTAL_ROWS)
            .filter(|row| {
                let id = row % ROW_GROUP_ROWS;
                (28..37).contains(&id) && id != 31 && *row != 32
            })
            .map(label)
            .collect::<Vec<_>>();
        let context = fixture.context();
        let plan = provider
            .scan(
                &context.state(),
                Some(&vec![1]),
                &[col("id").gt_eq(lit(28_i64)).and(col("id").lt(lit(37_i64)))],
                None,
            )
            .await
            .unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 4);
        let batches = collect(plan, context.task_ctx()).await.unwrap();
        let mut labels = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        labels.sort_unstable();
        assert_eq!(labels, expected, "indexes={write_indexes}");
    }
}

#[tokio::test]
async fn multiple_files_distribute_rows_across_independent_consumers() {
    const ROWS_PER_FILE: i64 = ROWS * 4;
    const ROW_GROUP_ROWS: usize = 128;
    let fixture = Fixture::new(false).await;
    let mut files = Vec::new();
    let padding = "x".repeat(2048);
    let base = datafusion::arrow::compute::concat_batches(
        &fixture.batch.schema(),
        std::iter::repeat_n(&fixture.batch, 4),
    )
    .unwrap();
    for file in 0..8 {
        let first = file * ROWS_PER_FILE;
        let mut columns = base.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from_iter_values(first..first + ROWS_PER_FILE));
        columns[1] = Arc::new(StringArray::from_iter_values(
            (first..first + ROWS_PER_FILE).map(|id| format!("{id:08}{padding}")),
        ));
        let batch = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
        let output = fixture
            .table
            .file_io()
            .new_output(format!(
                "{}/rows-{file}.parquet",
                fixture.table.metadata().location()
            ))
            .unwrap();
        let mut writer = ParquetWriterBuilder::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_statistics_enabled(EnabledStatistics::None)
                .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
                .build(),
            fixture.table.metadata().current_schema().clone(),
        )
        .build(output)
        .await
        .unwrap();
        writer.write(&batch).await.unwrap();
        files.extend(writer.close().await.unwrap().into_iter().map(|mut file| {
            file.content(DataContentType::Data)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .build()
                .unwrap()
        }));
    }
    let tx = Transaction::new(&fixture.table);
    let table = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .unwrap()
        .commit(&fixture.catalog)
        .await
        .unwrap();
    let provider = IcebergTableProvider::try_new(table).unwrap();
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(14)
            .with_batch_size(19),
    );
    let projection = vec![0];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await
        .unwrap();
    let partitions = collect_partitioned(plan, context.task_ctx()).await.unwrap();
    assert_eq!(partitions.len(), 14);
    let loads = partitions
        .iter()
        .map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>())
        .collect::<Vec<_>>();
    assert!(loads.iter().all(|rows| *rows > 0));
    let mut ids = partitions
        .into_iter()
        .flatten()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, (0..ROWS_PER_FILE * 8).collect::<Vec<_>>());
}

#[tokio::test]
async fn pushed_and_residual_filters_match_datafusion() {
    for format in [DataFileFormat::Parquet, DataFileFormat::Vortex] {
        assert_pushed_and_residual_filters_match_datafusion(format).await;
    }
}

async fn assert_pushed_and_residual_filters_match_datafusion(format: DataFileFormat) {
    let fixture = Fixture::new_with_format(true, format).await;
    let context = fixture.context();
    context
        .register_table(
            "ice",
            Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap()),
        )
        .unwrap();
    context
        .register_table(
            "reference",
            Arc::new(
                MemTable::try_new(fixture.batch.schema(), vec![vec![fixture.batch.clone()]])
                    .unwrap(),
            ),
        )
        .unwrap();
    for filter in [
        "id BETWEEN 17 AND 142",
        "id IN (1, 8, 130, NULL)",
        "flag IS NULL OR label LIKE '%1%'",
        "NOT (id > 12 AND label LIKE '%1%')",
        "flag != false",
        "label LIKE 'row-1%' AND id > 115",
        "CAST(id AS VARCHAR) = '128'",
        "payload.code BETWEEN 12 AND 30",
        "payload IS NOT NULL",
        "payload IS NULL OR id > 200",
        "special_float > 1.0",
        "special_float < 1.0",
        "NOT (special_float < 1.0)",
    ] {
        let mut results = Vec::new();
        for name in ["ice", "reference"] {
            let batches = context
                .sql(&format!(
                    "SELECT label, id FROM {name} WHERE {filter} ORDER BY id"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            results.push(pretty_format_batches(&batches).unwrap().to_string());
        }
        assert_eq!(
            results[0], results[1],
            "format: {format:?}, filter: {filter}"
        );
    }
    let keys = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "join_key",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![5, 19, 205]))],
    )
    .unwrap();
    context
        .register_table(
            "keys",
            Arc::new(MemTable::try_new(keys.schema(), vec![vec![keys]]).unwrap()),
        )
        .unwrap();
    let plan = context
        .sql("SELECT data.id FROM keys JOIN ice data ON keys.join_key = data.id ORDER BY data.id")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![5, 19, 205]);
}

#[tokio::test]
async fn deletes_apply_across_split_ranges_and_unprojected_keys() {
    for format in [DataFileFormat::Parquet, DataFileFormat::Vortex] {
        assert_deletes_apply_across_split_ranges_and_unprojected_keys(format).await;
    }
}

async fn assert_deletes_apply_across_split_ranges_and_unprojected_keys(format: DataFileFormat) {
    let mut fixture = Fixture::new_with_format(true, format).await;
    fixture.add_deletes().await;
    let provider = Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap());
    let context = fixture.context();
    context.register_table("ice", provider.clone()).unwrap();
    let removed = [0, 31, 32, 97, 128, 255, 256];
    for (filter, expected) in [
        (
            "true",
            (0..ROWS)
                .filter(|id| !removed.contains(id))
                .collect::<Vec<_>>(),
        ),
        (
            "id >= 64 AND id < 200",
            (64..200).filter(|id| !removed.contains(id)).collect(),
        ),
    ] {
        let batches = context
            .sql(&format!("SELECT id FROM ice WHERE {filter} ORDER BY id"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    let projection = vec![1];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await
        .unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    let mut labels = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    labels.sort_unstable();
    let mut expected = (0..ROWS)
        .filter(|id| !removed.contains(id))
        .map(|id| (id % 11 != 0).then(|| format!("row-{id}")))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(labels, expected);
}

#[tokio::test]
async fn added_columns_and_empty_snapshots_keep_their_schema() {
    for format in [DataFileFormat::Parquet, DataFileFormat::Vortex] {
        assert_added_columns_and_empty_snapshots_keep_their_schema(format).await;
    }
}

async fn assert_added_columns_and_empty_snapshots_keep_their_schema(format: DataFileFormat) {
    let mut fixture = Fixture::new_with_format(true, format).await;
    let tx = Transaction::new(&fixture.table);
    let table = tx
        .upgrade_table_version()
        .set_format_version(FormatVersion::V3)
        .apply(tx)
        .unwrap()
        .commit(&fixture.catalog)
        .await
        .unwrap();
    let tx = Transaction::new(&table);
    let table = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "new_column",
            Type::Primitive(PrimitiveType::String),
        ))
        .add_column(AddColumn::required(
            "defaulted",
            Type::Primitive(PrimitiveType::Int),
            Literal::int(7),
        ))
        .add_column(AddColumn::required(
            "default_string",
            PrimitiveType::String.into(),
            Literal::string("fallback"),
        ))
        .apply(tx)
        .unwrap()
        .commit(&fixture.catalog)
        .await
        .unwrap();
    fixture.table = table;
    fixture.evolve_field(1, |field| field.name = "renamed_label".to_owned());
    let provider = Arc::new(
        IcebergTableProvider::try_new(fixture.table.clone())
            .unwrap()
            .with_exact_filter_pushdown(true),
    );
    for filter in [
        col("new_column").is_null(),
        col("new_column").is_not_null(),
        col("defaulted").eq(lit(7_i32)),
        col("renamed_label").like(lit("row-%")),
    ] {
        assert!(!matches!(
            provider
                .supports_filters_pushdown(&[&filter])
                .unwrap()
                .as_slice(),
            [datafusion::logical_expr::TableProviderFilterPushDown::Exact]
        ));
    }
    let context = fixture.context();
    let projection = vec![6];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(
        (
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            batches
                .iter()
                .map(|b| b.column(0).null_count())
                .sum::<usize>()
        ),
        (ROWS as usize, ROWS as usize)
    );
    context.register_table("evolved", provider.clone()).unwrap();
    let missing = context
        .sql("SELECT id FROM evolved WHERE new_column IS NOT NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(missing.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    for filter in [
        "defaulted = 7",
        "defaulted > 6",
        "default_string = 'fallback'",
    ] {
        let batches = context
            .sql(&format!(
                "SELECT id FROM evolved WHERE {filter} ORDER BY id"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, (0..ROWS).collect::<Vec<_>>(), "filter: {filter}");
    }
    let batches = context
        .sql("SELECT renamed_label, default_string FROM evolved ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let labels = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected = fixture
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(str::to_owned))
        .collect::<Vec<_>>();
    assert_eq!(labels, expected);
    for batch in batches {
        assert!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .all(|value| value == Some("fallback"))
        );
    }
    let projection = vec![8, 0];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await
        .unwrap();
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("default_string", 0));
    let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![key.clone()],
        Arc::new(PhysicalLiteral::new(ScalarValue::Boolean(Some(true)))),
    ));
    dynamic
        .update(Arc::new(BinaryExpr::new(
            key,
            Operator::Eq,
            Arc::new(PhysicalLiteral::new(ScalarValue::Utf8(Some(
                "fallback".to_owned(),
            )))),
        )))
        .unwrap();
    let plan = with_dynamic_filter(plan, dynamic, &context);
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize
    );

    let empty = Fixture::new_with_format(false, format).await;
    let provider = IcebergTableProvider::try_new(empty.table).unwrap();
    let plan = provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    assert_eq!(plan.schema(), provider.schema());
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    assert!(collect(plan, context.task_ctx()).await.unwrap().is_empty());
}

#[tokio::test]
async fn footer_bounds_reach_projected_provider_statistics() {
    let mut fixture = Fixture::new_with_id_type(true, DataType::Int32).await;
    fixture.evolve_field(0, |field| {
        field.name = "renamed_id".to_owned();
        *field.field_type = PrimitiveType::Long.into();
    });
    let context = fixture.context();
    let provider = IcebergTableProvider::try_new(fixture.table).unwrap();
    let plan = provider
        .scan(&context.state(), Some(&vec![1, 0]), &[], None)
        .await
        .unwrap();
    let statistics = plan
        .statistics_from_inputs(&[], &StatisticsArgs::new())
        .unwrap();
    assert_eq!(statistics.num_rows, Precision::Exact(ROWS as usize));
    let label = &statistics.column_statistics[0];
    assert_eq!(label.min_value, Precision::Absent);
    assert_eq!(label.max_value, Precision::Absent);
    let id = &statistics.column_statistics[1];
    assert_eq!(
        id.min_value,
        Precision::Inexact(ScalarValue::Int64(Some(1)))
    );
    assert_eq!(
        id.max_value,
        Precision::Inexact(ScalarValue::Int64(Some(256)))
    );
    assert_eq!(id.distinct_count, Precision::Absent);
    assert_eq!(id.null_count, Precision::Absent);
}

#[tokio::test]
async fn promoted_integer_predicates_preserve_out_of_range_literals() {
    let mut fixture = Fixture::new_with_id_type(true, DataType::Int32).await;
    fixture.evolve_field(0, |field| *field.field_type = PrimitiveType::Long.into());
    let provider = Arc::new(
        IcebergTableProvider::try_new(fixture.table.clone())
            .unwrap()
            .with_exact_filter_pushdown(true),
    );
    let context = fixture.context();
    context
        .register_table("promoted", provider.clone())
        .unwrap();
    let unfiltered = context
        .sql("SELECT * FROM promoted")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        unfiltered.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize
    );
    assert_eq!(
        unfiltered
            .iter()
            .map(|batch| batch.column(0).null_count())
            .sum::<usize>(),
        20
    );
    assert_eq!(provider.schema().field(0).data_type(), &DataType::Int64);
    assert!(!matches!(
        provider
            .supports_filters_pushdown(&[&col("id").lt(lit(2147483648_i64))])
            .unwrap()
            .as_slice(),
        [datafusion::logical_expr::TableProviderFilterPushDown::Exact]
    ));
    for (filter, expected_rows) in [
        (col("id").lt(lit(2147483648_i64)), ROWS as usize - 20),
        (col("id").gt(lit(2147483648_i64)), 0),
    ] {
        let batches = context
            .table("promoted")
            .await
            .unwrap()
            .filter(filter)
            .unwrap()
            .select_columns(&["label"])
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows
        );
    }
    context
        .register_table(
            "reference",
            Arc::new(MemTable::try_new(provider.schema(), vec![unfiltered]).unwrap()),
        )
        .unwrap();
    for filter in [
        "id < 2147483648",
        "id >= -2147483649",
        "id != 2147483648",
        "id IN (4, 97, 2147483648)",
        "id NOT IN (4, 97, 2147483648)",
        "id IN (-2147483649, 4, 97)",
        "id NOT IN (-2147483649, 4, 97)",
        "id IS NULL OR id < 2147483648",
    ] {
        let mut results = Vec::new();
        for name in ["promoted", "reference"] {
            let batches = context
                .sql(&format!(
                    "SELECT id, label FROM {name} WHERE {filter} ORDER BY id NULLS FIRST, label"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            results.push(pretty_format_batches(&batches).unwrap().to_string());
        }
        assert_eq!(results[0], results[1], "filter: {filter}");
    }
}

#[tokio::test]
async fn dynamic_filters_remain_live_after_projection_and_field_rename() {
    let mut fixture = Fixture::new(true).await;
    fixture.add_deletes().await;
    fixture.evolve_field(0, |field| field.name = "key".to_owned());
    let provider = IcebergTableProvider::try_new(fixture.table.clone()).unwrap();
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(2)
            .with_batch_size(8),
    );
    let projection = vec![1, 0];
    let plan = provider
        .scan(
            &context.state(),
            Some(&projection),
            &[col("key").gt(lit(50_i64))],
            None,
        )
        .await
        .unwrap();
    let baseline_plan = provider
        .scan(
            &context.state(),
            Some(&projection),
            &[col("key").gt(lit(50_i64))],
            None,
        )
        .await
        .unwrap();
    let baseline = baseline_plan
        .execute(0, context.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let baseline_rows = baseline.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert!(baseline_rows > 32);
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 1));
    let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![key.clone()],
        Arc::new(PhysicalLiteral::new(ScalarValue::Boolean(Some(true)))),
    ));
    let plan = with_dynamic_filter(plan, dynamic.clone(), &context);
    let mut visited = false;
    plan.apply_expressions(&mut |expression| {
        visited |= expression.expression_id() == dynamic.expression_id();
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert!(visited);
    let mut input = plan.execute(0, context.task_ctx()).unwrap();
    let first = input.next().await.unwrap().unwrap();
    dynamic
        .update(Arc::new(BinaryExpr::new(
            key,
            Operator::GtEq,
            Arc::new(PhysicalLiteral::new(ScalarValue::Int64(Some(200)))),
        )))
        .unwrap();
    let later = input.try_collect::<Vec<_>>().await.unwrap();
    assert!(
        first.num_rows() + later.iter().map(RecordBatch::num_rows).sum::<usize>() < baseline_rows
    );
    dynamic.mark_complete();
    let plan = provider
        .scan(
            &context.state(),
            Some(&projection),
            &[col("key").gt(lit(50_i64))],
            None,
        )
        .await
        .unwrap();
    let plan = with_dynamic_filter(plan, dynamic, &context);
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    let mut keys = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, (200..255).collect::<Vec<_>>());
}

#[tokio::test]
async fn vortex_single_file_scans_preserve_parallel_rows_and_projections() {
    let fixture = Fixture::new_with_format(true, DataFileFormat::Vortex).await;
    let provider = IcebergTableProvider::try_new(fixture.table.clone()).unwrap();
    let context = fixture.context();
    let projection = vec![3, 0, 1, 5];
    let plan = provider
        .scan(&context.state(), Some(&projection), &[], None)
        .await
        .unwrap();
    let expected_schema = provider.schema().project(&projection).unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    let partitions = collect_partitioned(plan, context.task_ctx()).await.unwrap();
    let mut ids = Vec::new();
    for batch in partitions.into_iter().flatten() {
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let labels = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let blobs = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        assert_eq!(
            labels
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>(),
            keys.values()
                .iter()
                .map(|id| (id % 11 != 0).then(|| format!("row-{id}")))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            blobs
                .iter()
                .map(|value| value.map(<[u8]>::to_vec))
                .collect::<Vec<_>>(),
            keys.values()
                .iter()
                .map(|id| (id % 11 != 0).then(|| id.to_le_bytes().to_vec()))
                .collect::<Vec<_>>()
        );
        ids.extend(keys.values().iter().copied());
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..ROWS).collect::<Vec<_>>());

    let plan = provider
        .scan(&context.state(), Some(&vec![]), &[], None)
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(
        (
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            batches.iter().all(|batch| batch.num_columns() == 0),
        ),
        (ROWS as usize, true)
    );
}

#[tokio::test]
async fn mixed_data_formats_preserve_rows_filters_and_deletes() {
    for (first, second) in [
        (DataFileFormat::Parquet, DataFileFormat::Vortex),
        (DataFileFormat::Vortex, DataFileFormat::Parquet),
    ] {
        let mut fixture = Fixture::new_with_format(true, first).await;
        let mut columns = fixture.batch.columns().to_vec();
        columns[0] = Arc::new(Int64Array::from_iter_values(ROWS..ROWS * 2));
        let extra = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
        let path = format!("{}/extra.{second}", fixture.table.metadata().location());
        fixture.write_batch(&path, second, &extra).await;
        fixture.add_deletes().await;
        let removed = [0, 31, 32, 97, 128, 255, 256];
        let provider = Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap());
        let context = fixture.context();
        let plan = provider
            .scan(&context.state(), Some(&vec![0]), &[], None)
            .await
            .unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 4);
        let partitions = collect_partitioned(plan, context.task_ctx()).await.unwrap();
        assert!(
            partitions
                .iter()
                .filter(|batches| !batches.is_empty())
                .count()
                > 1
        );
        let mut actual: Vec<_> = partitions
            .into_iter()
            .flatten()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        actual.sort_unstable();
        assert_eq!(
            actual,
            (0..ROWS * 2)
                .filter(|id| !removed.contains(id))
                .collect::<Vec<_>>()
        );

        let mask = BooleanArray::from_iter((0..ROWS).map(|id| Some(!removed.contains(&id))));
        let retained =
            datafusion::arrow::compute::filter_record_batch(&fixture.batch, &mask).unwrap();
        context.register_table("ice", provider).unwrap();
        context
            .register_table(
                "reference",
                Arc::new(
                    MemTable::try_new(fixture.batch.schema(), vec![vec![retained, extra]]).unwrap(),
                ),
            )
            .unwrap();
        for filter in [
            "id BETWEEN 200 AND 300",
            "flag IS NULL OR label LIKE '%1%'",
            "payload.code BETWEEN 12 AND 30 OR id >= 500",
        ] {
            let mut results = Vec::new();
            for name in ["ice", "reference"] {
                let batches = context
                    .sql(&format!(
                        "SELECT label, id FROM {name} WHERE {filter} ORDER BY id"
                    ))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                results.push(pretty_format_batches(&batches).unwrap().to_string());
            }
            assert_eq!(
                results[0], results[1],
                "first format: {first:?}, filter: {filter}"
            );
        }
    }
}

#[tokio::test]
async fn unsupported_data_formats_are_rejected_before_opening_files() {
    let mut fixture = Fixture::new(false).await;
    let path = fixture._root.path().join("unopened.avro");
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string_lossy().into_owned())
        .file_format(DataFileFormat::Avro)
        .file_size_in_bytes(1)
        .record_count(1)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .unwrap();
    fixture.append_files(vec![file]).await;
    let provider = IcebergTableProvider::try_new(fixture.table.clone()).unwrap();
    let error = provider
        .scan(&fixture.context().state(), None, &[], None)
        .await
        .unwrap_err();
    assert!(matches!(error, DataFusionError::NotImplemented(_)));
    assert!(!path.exists());
}

async fn flat_vortex_fixture() -> Fixture {
    let mut fixture = Fixture::new(false).await;
    let projected = fixture.batch.schema().project(&[0, 1, 4]).unwrap();
    let schema = arrow_schema_to_schema_auto_assign_ids(&projected).unwrap();
    let table = fixture
        .catalog
        .create_table(
            &NamespaceIdent::new("test".to_owned()),
            TableCreation::builder()
                .name("flat".to_owned())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let arrow_schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap());
    fixture.batch = RecordBatch::try_new(arrow_schema, vec![
        fixture.batch.column(0).clone(),
        fixture.batch.column(1).clone(),
        Arc::new(Float64Array::from_iter((0..ROWS).map(|id| match id % 8 {
            0 => None,
            1 => Some(f64::NAN),
            2 => Some(f64::INFINITY),
            3 => Some(f64::NEG_INFINITY),
            4 => Some(-0.0),
            5 => Some(0.0),
            6 => Some(-1.0),
            _ => Some(1.0),
        }))),
    ])
    .unwrap();
    fixture.data_path = format!("{}/flat.vortex", table.metadata().location());
    fixture.table = table;
    let path = fixture.data_path.clone();
    let batch = fixture.batch.clone();
    fixture
        .write_batch(&path, DataFileFormat::Vortex, &batch)
        .await;
    fixture
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_vortex_scans_preserve_parallel_queries_and_evolution() {
    let mut fixture = flat_vortex_fixture().await;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .update_schema()
        .add_column(AddColumn::optional("missing", PrimitiveType::String.into()))
        .apply(tx)
        .unwrap()
        .commit(&fixture.catalog)
        .await
        .unwrap();
    fixture.evolve_field(1, |field| field.name = "renamed_label".to_owned());
    let schema =
        Arc::new(schema_to_arrow_schema(fixture.table.metadata().current_schema()).unwrap());
    let mut columns = fixture.batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![
        None::<&str>;
        ROWS as usize
    ])));
    let reference = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let context = fixture.context();
    let provider = Arc::new(
        IcebergTableProvider::try_new(fixture.table.clone())
            .unwrap()
            .with_exact_filter_pushdown(true),
    );
    for projection in [vec![], vec![0]] {
        let projected = provider
            .scan(&context.state(), Some(&projection), &[], None)
            .await
            .unwrap();
        let statistics = projected
            .statistics_from_inputs(&[], &StatisticsArgs::new())
            .unwrap();
        assert_eq!(statistics.total_byte_size, Precision::Absent);
    }
    let plan = provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(display.contains("VortexScanSource"), "{display}");
    assert!(!display.contains("IcebergScan:"), "{display}");
    assert_eq!(plan.output_partitioning().partition_count(), 4);
    let partitions = collect_partitioned(plan, context.task_ctx()).await.unwrap();
    assert_eq!(
        partitions
            .iter()
            .flatten()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        ROWS as usize
    );
    assert!(
        partitions
            .iter()
            .flatten()
            .all(|batch| batch.schema() == provider.schema())
    );
    context.register_table("ice", provider).unwrap();
    context
        .register_table(
            "reference",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![reference.clone()]]).unwrap()),
        )
        .unwrap();
    for query in [
        "SELECT id, special_float * 2.0 AS doubled, special_float + 1.0 AS shifted FROM {} ORDER BY id",
        "SELECT id, special_float / 0.0 AS divided FROM {} ORDER BY id",
        "SELECT id, 1.0 / (special_float + 0.0) AS inverse FROM {} ORDER BY id",
        "SELECT id, special_float * 0.0 AS zero_product FROM {} ORDER BY id",
        "SELECT id, renamed_label, missing FROM {} WHERE id >= 17 AND id < 240 AND (renamed_label LIKE 'row-1%' OR renamed_label IS NULL) ORDER BY id",
        "SELECT id, special_float FROM {} WHERE special_float >= 0.0 OR special_float IS NULL ORDER BY id",
        "SELECT id FROM {} WHERE special_float = CAST('NaN' AS DOUBLE) ORDER BY id",
        "SELECT id, id + 9223372036854775807 AS wrapped FROM {} ORDER BY id",
        "SELECT id, renamed_label FROM {} WHERE id >= 11 ORDER BY id LIMIT 17",
        "SELECT id, missing FROM {} WHERE missing IS NULL ORDER BY id",
    ] {
        let plan = context
            .sql(&query.replace("{}", "ice"))
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();
        assert!(display.contains("VortexScanSource"), "{display}");
        if query.contains(" AS divided ") {
            assert!(
                display
                    .lines()
                    .any(|line| line.contains("VortexScanSource") && line.contains("divided")),
                "{display}"
            );
        }
        if query.contains("9223372036854775807") {
            // The generic bridge evaluates wrapping arithmetic with DF inside
            // the scan, rather than using Vortex's checked integer operation.
            assert!(
                display.lines().any(|line| line.contains("VortexScanSource")
                    && line.contains("wrapped")
                    && line.contains("coldfusion.datafusion_expr")),
                "{display}"
            );
        }
        let actual = collect(plan, context.task_ctx()).await.unwrap();
        let expected = context
            .sql(&query.replace("{}", "reference"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string(),
            "{query}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_vortex_numeric_casts_push_into_filters_and_projections() {
    let mut fixture = Fixture::new(false).await;
    let schema = arrow_schema_to_schema_auto_assign_ids(&Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("n", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]))
    .unwrap();
    fixture.table = fixture
        .catalog
        .create_table(
            &NamespaceIdent::new("test".to_owned()),
            TableCreation::builder()
                .name("numeric_casts".to_owned())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let values = [
        Some(i64::MIN),
        Some(i64::MAX),
        Some((1 << 53) + 1),
        Some(-(1 << 53) - 1),
        Some(0),
        None,
    ];
    fixture.batch = RecordBatch::try_new(
        Arc::new(schema_to_arrow_schema(fixture.table.metadata().current_schema()).unwrap()),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(Int64Array::from_iter(
                (0..ROWS as usize).map(|row| values[row % values.len()]),
            )),
            Arc::new(BooleanArray::from_iter(
                (0..ROWS).map(|row| (row % 5 != 0).then_some(row % 2 == 0)),
            )),
        ],
    )
    .unwrap();
    let path = format!("{}/casts.vortex", fixture.table.metadata().location());
    let batch = fixture.batch.clone();
    fixture
        .write_batch(&path, DataFileFormat::Vortex, &batch)
        .await;
    let context = fixture.context();
    context
        .register_table(
            "ice",
            Arc::new(
                IcebergTableProvider::try_new(fixture.table.clone())
                    .unwrap()
                    .with_exact_filter_pushdown(true),
            ),
        )
        .unwrap();
    context
        .register_table(
            "reference",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    for query in [
        "SELECT id, CAST(n AS DOUBLE) AS rounded, CAST(flag AS DOUBLE) AS flag_number FROM {} ORDER BY id",
        "SELECT id FROM {} WHERE CAST(flag AS DOUBLE) IS NOT NULL ORDER BY id",
    ] {
        let plan = context
            .sql(&query.replace("{}", "ice"))
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();
        let scan = display
            .lines()
            .find(|line| line.contains("VortexScanSource"))
            .expect("native Vortex source");
        assert!(
            scan.contains("cast("),
            "casts must execute natively in the scan: {display}"
        );
        assert!(
            !scan.contains("coldfusion.datafusion_expr"),
            "casts must not use the Arrow callback: {display}"
        );
        let actual = collect(plan, context.task_ctx()).await.unwrap();
        let expected = context
            .sql(&query.replace("{}", "reference"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(actual[0].schema(), expected[0].schema());
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string(),
            "{query}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_vortex_decimal_arithmetic_matches_datafusion_at_precision_bounds() {
    use datafusion::arrow::array::Decimal128Array;

    let mut fixture = Fixture::new(false).await;
    let schema = arrow_schema_to_schema_auto_assign_ids(&Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Decimal128(18, 2), true),
        Field::new("b", DataType::Decimal128(18, 2), true),
    ]))
    .unwrap();
    let table = fixture
        .catalog
        .create_table(
            &NamespaceIdent::new("test".to_owned()),
            TableCreation::builder()
                .name("decimal_arithmetic".to_owned())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let maximum = 10_i128.pow(18) - 1;
    let decimals = |values: &[Option<i128>]| -> ArrayRef {
        let values =
            Decimal128Array::from_iter((0..ROWS as usize).map(|row| values[row % values.len()]))
                .with_precision_and_scale(18, 2)
                .unwrap();
        values.validate_decimal_precision(18).unwrap();
        Arc::new(values)
    };
    fixture.batch = RecordBatch::try_new(
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).unwrap()),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            decimals(&[
                Some(maximum),
                Some(-maximum),
                Some(0),
                None,
                Some(maximum),
                Some(-maximum),
                Some(1),
                Some(-1),
            ]),
            decimals(&[
                Some(maximum),
                Some(-maximum),
                Some(maximum),
                Some(0),
                Some(-maximum),
                Some(maximum),
                None,
                Some(0),
            ]),
        ],
    )
    .unwrap();
    fixture.data_path = format!("{}/decimal.vortex", table.metadata().location());
    fixture.table = table;
    let path = fixture.data_path.clone();
    let batch = fixture.batch.clone();
    fixture
        .write_batch(&path, DataFileFormat::Vortex, &batch)
        .await;

    let context = fixture.context();
    context
        .register_table(
            "ice",
            Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap()),
        )
        .unwrap();
    context
        .register_table(
            "reference",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    let query =
        "SELECT id, a + b AS added, a - b AS subtracted, a * b AS multiplied FROM {} ORDER BY id";
    let plan = context
        .sql(&query.replace("{}", "ice"))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let display = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        display.lines().any(|line| line.contains("VortexScanSource")
            && ["added", "subtracted", "multiplied"]
                .iter()
                .all(|name| line.contains(name))),
        "decimal arithmetic must be computed in the native scan: {display}"
    );
    let actual = collect(plan, context.task_ctx()).await.unwrap();
    let expected = context
        .sql(&query.replace("{}", "reference"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected_schema = expected[0].schema();
    assert_eq!(
        expected_schema.field(1).data_type(),
        &DataType::Decimal128(19, 2)
    );
    assert_eq!(
        expected_schema.field(2).data_type(),
        &DataType::Decimal128(19, 2)
    );
    assert_eq!(
        expected_schema.field(3).data_type(),
        &DataType::Decimal128(37, 4)
    );
    assert!(actual.iter().all(|batch| batch.schema() == expected_schema));
    assert_eq!(
        actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize
    );
    assert_eq!(
        pretty_format_batches(&actual).unwrap().to_string(),
        pretty_format_batches(&expected).unwrap().to_string(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_vortex_limits_keep_parallel_scan_outputs_after_optimization() {
    use datafusion::arrow::compute::SortOptions;
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::{ExecutionPlan, SortOrderPushdownResult};

    fn assert_parallel_sources(plan: &dyn ExecutionPlan, config: &ConfigOptions) -> usize {
        let mut sources = 0;
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            let display = displayable(scan).indent(true).to_string();
            assert!(display.contains("VortexScanSource"), "{display}");
            assert!(
                scan.properties().output_partitioning().partition_count() >= 2,
                "{display}"
            );
            let source = scan.data_source();
            assert!(source.with_fetch(Some(17)).is_none());
            assert!(source.fetch().is_none());
            assert!(source.repartitioned(1, 0, None).unwrap().is_none());
            let order = vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("id", 0)),
                options: SortOptions::default(),
            }];
            assert!(
                source
                    .repartitioned(4, 0, LexOrdering::new(order.clone()))
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                source.try_pushdown_sort(&order).unwrap(),
                SortOrderPushdownResult::Unsupported
            ));
            assert!(source.with_preserve_order(true).is_none());
            // Physical filter rewrites must also retain the wrapper, rather than handing the
            // optimizer an unguarded official source that can later collapse to one partition.
            let filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 0)),
                Operator::GtEq,
                Arc::new(PhysicalLiteral::new(ScalarValue::Int64(Some(11)))),
            ));
            let pushed = source.try_pushdown_filters(vec![filter], config).unwrap();
            assert!(matches!(pushed.filters.as_slice(), [PushedDown::Yes]));
            let updated = pushed.updated_node.expect("native filter must push down");
            assert!(updated.output_partitioning().partition_count() >= 2);
            assert!(updated.with_fetch(Some(17)).is_none());
            assert!(updated.repartitioned(1, 0, None).unwrap().is_none());
            sources += 1;
        }
        sources
            + plan
                .children()
                .into_iter()
                .map(|child| assert_parallel_sources(child.as_ref(), config))
                .sum::<usize>()
    }

    let fixture = flat_vortex_fixture().await;
    for target in [1, 4] {
        let context = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(target)
                .with_batch_size(19),
        );
        context
            .register_table(
                "ice",
                Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap()),
            )
            .unwrap();
        context
            .register_table(
                "reference",
                Arc::new(
                    MemTable::try_new(fixture.batch.schema(), vec![vec![fixture.batch.clone()]])
                        .unwrap(),
                ),
            )
            .unwrap();
        for query in [
            "SELECT id FROM {} LIMIT 17",
            "SELECT id, special_float + 1.0 AS shifted FROM {} WHERE id >= 11 ORDER BY id LIMIT 17",
        ] {
            let plan = context
                .sql(&query.replace("{}", "ice"))
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            assert_eq!(
                assert_parallel_sources(plan.as_ref(), context.state().config_options()),
                1
            );
            let display = displayable(plan.as_ref()).indent(true).to_string();
            if query.contains("shifted") {
                assert!(
                    display
                        .lines()
                        .any(|line| line.contains("VortexScanSource") && line.contains("shifted")),
                    "{display}"
                );
            }
            let actual = collect(plan, context.task_ctx()).await.unwrap();
            assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 17);
            if query.contains("ORDER BY") {
                let expected = context
                    .sql(&query.replace("{}", "reference"))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert_eq!(
                    pretty_format_batches(&actual).unwrap().to_string(),
                    pretty_format_batches(&expected).unwrap().to_string()
                );
            } else {
                // An unordered global LIMIT can select any distinct input rows.
                let mut ids = actual
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<_>>();
                ids.sort_unstable();
                ids.dedup();
                assert_eq!(ids.len(), 17);
                assert!(ids.iter().all(|id| (0..ROWS).contains(id)));
            }
        }
    }
}

#[tokio::test]
async fn shared_source_reads_nested_deletes_and_mixed_files() {
    for scenario in ["nested", "deletes", "mixed"] {
        let mut fixture = if scenario == "nested" {
            Fixture::new_with_format(true, DataFileFormat::Vortex).await
        } else {
            flat_vortex_fixture().await
        };
        let mut reference = vec![fixture.batch.clone()];
        if scenario == "mixed" {
            let mut columns = fixture.batch.columns().to_vec();
            columns[0] = Arc::new(Int64Array::from_iter_values(ROWS..ROWS * 2));
            let extra = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
            let path = format!("{}/extra.parquet", fixture.table.metadata().location());
            fixture
                .write_batch(&path, DataFileFormat::Parquet, &extra)
                .await;
            reference.push(extra);
        }
        if scenario != "nested" {
            fixture.add_deletes().await;
            let removed = [0, 31, 32, 97, 128, 255, 256];
            let mask = BooleanArray::from_iter((0..ROWS).map(|id| Some(!removed.contains(&id))));
            reference[0] =
                datafusion::arrow::compute::filter_record_batch(&fixture.batch, &mask).unwrap();
        }
        let context = fixture.context();
        let provider = Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap());
        let plan = provider
            .scan(&context.state(), None, &[], None)
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            display.contains("VortexScanSource"),
            "{scenario}: {display}"
        );
        assert_eq!(plan.output_partitioning().partition_count(), 4);
        context.register_table("ice", provider).unwrap();
        context
            .register_table(
                "reference",
                Arc::new(
                    MemTable::try_new(fixture.batch.schema(), vec![reference.clone()]).unwrap(),
                ),
            )
            .unwrap();
        let mut results = Vec::new();
        for name in ["ice", "reference"] {
            let batches = context
                .sql(&format!(
                    "SELECT id, label, special_float FROM {name} \
                     WHERE id >= 17 AND (label LIKE 'row-1%' OR label IS NULL) ORDER BY id"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            results.push(pretty_format_batches(&batches).unwrap().to_string());
        }
        assert_eq!(results[0], results[1], "{scenario}");
    }
}

async fn exact_filter_fixture(format: DataFileFormat) -> Fixture {
    use datafusion::arrow::array::{Date32Array, Decimal128Array};

    let mut fixture = Fixture::new(false).await;
    let mut fields = fixture
        .batch
        .schema()
        .project(&[0, 1, 2])
        .unwrap()
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone().with_nullable(false)))
        .collect::<Vec<_>>();
    fields.extend([
        Arc::new(Field::new("price", DataType::Decimal128(15, 2), false)),
        Arc::new(Field::new("day", DataType::Date32, false)),
        Arc::new(Field::new("special_float", DataType::Float64, true)),
    ]);
    let schema = arrow_schema_to_schema_auto_assign_ids(&Schema::new(fields)).unwrap();
    fixture.table = fixture
        .catalog
        .create_table(
            &NamespaceIdent::new("test".to_owned()),
            TableCreation::builder()
                .name("exact".to_owned())
                .schema(schema)
                .build(),
        )
        .await
        .unwrap();
    let schema =
        Arc::new(schema_to_arrow_schema(fixture.table.metadata().current_schema()).unwrap());
    fixture.batch = RecordBatch::try_new(schema, vec![
        fixture.batch.column(0).clone(),
        Arc::new(StringArray::from_iter_values(
            (0..ROWS).map(|id| format!("row-{id}")),
        )),
        Arc::new(BooleanArray::from(
            (0..ROWS).map(|id| id % 2 == 0).collect::<Vec<_>>(),
        )),
        Arc::new(
            Decimal128Array::from_iter_values(
                (0..ROWS).map(|id| i128::from(id % 101 - 50) * 100 + i128::from(id % 5)),
            )
            .with_precision_and_scale(15, 2)
            .unwrap(),
        ),
        Arc::new(Date32Array::from_iter_values(
            (0..ROWS).map(|id| (id % 31 - 15) as i32),
        )),
        fixture.batch.column(4).clone(),
    ])
    .unwrap();
    fixture.data_path = format!("{}/exact.{format}", fixture.table.metadata().location());
    let path = fixture.data_path.clone();
    let batch = fixture.batch.clone();
    fixture.write_batch(&path, format, &batch).await;
    fixture
}

#[tokio::test]
async fn exact_filters_enforce_required_decimal_date_predicates_without_residuals() {
    use datafusion::logical_expr::TableProviderFilterPushDown;

    for (first, second) in [
        (DataFileFormat::Parquet, None),
        (DataFileFormat::Vortex, None),
        (DataFileFormat::Parquet, Some(DataFileFormat::Vortex)),
    ] {
        let mut fixture = exact_filter_fixture(first).await;
        let mut reference = vec![fixture.batch.clone()];
        if let Some(second) = second {
            let mut columns = fixture.batch.columns().to_vec();
            columns[0] = Arc::new(Int64Array::from_iter_values(ROWS..ROWS * 2));
            let extra = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
            let path = format!("{}/extra.{second}", fixture.table.metadata().location());
            fixture.write_batch(&path, second, &extra).await;
            reference.push(extra);
        }
        fixture.add_deletes().await;
        let removed = [0, 31, 32, 97, 128, 255, 256];
        let mask = BooleanArray::from_iter((0..ROWS).map(|id| Some(!removed.contains(&id))));
        reference[0] =
            datafusion::arrow::compute::filter_record_batch(&fixture.batch, &mask).unwrap();
        fixture.evolve_field(1, |field| field.name = "renamed_label".to_owned());
        let schema =
            Arc::new(schema_to_arrow_schema(fixture.table.metadata().current_schema()).unwrap());
        let reference = reference
            .into_iter()
            .map(|batch| RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap())
            .collect();
        let provider = Arc::new(
            IcebergTableProvider::try_new(fixture.table.clone())
                .unwrap()
                .with_exact_filter_pushdown(true),
        );
        let context = fixture.context();
        context.register_table("ice", provider.clone()).unwrap();
        context
            .register_table(
                "reference",
                Arc::new(MemTable::try_new(schema, vec![reference]).unwrap()),
            )
            .unwrap();
        let price = lit(ScalarValue::Decimal128(Some(-500), 15, 2));
        let day = lit(ScalarValue::Date32(Some(-5)));
        for filter in [
            col("price")
                .gt(price.clone())
                .and(col("day").gt_eq(day.clone())),
            col("price").lt_eq(price).or(col("day").gt(day)),
            col("day").in_list(
                vec![
                    lit(ScalarValue::Date32(Some(-1))),
                    lit(ScalarValue::Date32(Some(1))),
                ],
                false,
            ),
            col("price").is_null().or(col("day").is_not_null()),
            col("renamed_label").like(lit("row-1%")),
            col("flag").eq(lit(true)),
        ] {
            assert!(matches!(
                provider
                    .supports_filters_pushdown(&[&filter])
                    .unwrap()
                    .as_slice(),
                [TableProviderFilterPushDown::Exact]
            ));
            let direct = provider
                .scan(
                    &context.state(),
                    Some(&vec![0]),
                    std::slice::from_ref(&filter),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                direct
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["id"]
            );
            let actual = collect(direct, context.task_ctx()).await.unwrap();
            let expected = context
                .table("reference")
                .await
                .unwrap()
                .filter(filter.clone())
                .unwrap()
                .select(vec![col("id")])
                .unwrap()
                .collect()
                .await
                .unwrap();
            let ids = |batches: &[RecordBatch]| {
                let mut values = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                values.sort_unstable();
                values
            };
            assert_eq!(ids(&actual), ids(&expected), "direct {first:?}: {filter}");
            let plan = context
                .table("ice")
                .await
                .unwrap()
                .filter(filter.clone())
                .unwrap()
                .select(vec![col("id")])
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let display = displayable(plan.as_ref()).indent(true).to_string();
            assert!(!display.contains("FilterExec"), "{display}");
            let mut nodes = vec![plan.clone()];
            while let Some(node) = nodes.pop() {
                if node.name() == "IcebergScan" {
                    assert_eq!(node.schema().fields().len(), 1, "{display}");
                }
                nodes.extend(node.children().into_iter().cloned());
            }
            let actual = collect(plan, context.task_ctx()).await.unwrap();
            assert_eq!(ids(&actual), ids(&expected), "planned {first:?}: {filter}");
        }
        for filter in [
            col("id").not_eq(lit(5_i64)),
            col("id").in_list(vec![lit(5_i64)], true),
            col("renamed_label").not_like(lit("row-%")),
            col("special_float").gt(lit(0.0_f64)),
        ] {
            assert!(!matches!(
                provider
                    .supports_filters_pushdown(&[&filter])
                    .unwrap()
                    .as_slice(),
                [TableProviderFilterPushDown::Exact]
            ));
        }
    }
}

#[tokio::test]
async fn historical_partition_specs_keep_exact_filters_residual() {
    use datafusion::logical_expr::TableProviderFilterPushDown;
    use iceberg::spec::{Transform, UnboundPartitionField, UnboundPartitionSpec};

    let fixture = Fixture::new(false).await;
    let filter = col("id").eq(lit(5_i64));
    let provider = IcebergTableProvider::try_new(fixture.table.clone())
        .unwrap()
        .with_exact_filter_pushdown(true);
    assert!(matches!(
        provider
            .supports_filters_pushdown(&[&filter])
            .unwrap()
            .as_slice(),
        [TableProviderFilterPushDown::Exact]
    ));
    let old_spec = UnboundPartitionSpec::builder()
        .add_partition_field(
            UnboundPartitionField::builder()
                .source_ids(vec![
                    fixture
                        .table
                        .metadata()
                        .current_schema()
                        .field_by_name("id")
                        .unwrap()
                        .id,
                ])
                .name("id_partition")
                .transform(Transform::Identity)
                .build()
                .unwrap(),
        )
        .unwrap()
        .build();
    let metadata = fixture
        .table
        .metadata()
        .clone()
        .into_builder(fixture.table.metadata_location().map(str::to_owned))
        .add_partition_spec(old_spec)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert!(metadata.default_partition_spec().fields().is_empty());
    assert!(
        metadata
            .partition_specs_iter()
            .any(|spec| !spec.fields().is_empty())
    );
    let table = Table::builder()
        .metadata(metadata)
        .identifier(fixture.table.identifier().clone())
        .file_io(fixture.table.file_io().clone())
        .runtime(Runtime::current())
        .build()
        .unwrap();
    let provider = IcebergTableProvider::try_new(table)
        .unwrap()
        .with_exact_filter_pushdown(true);
    assert!(matches!(
        provider
            .supports_filters_pushdown(&[&filter])
            .unwrap()
            .as_slice(),
        [TableProviderFilterPushDown::Inexact]
    ));
}

#[test]
fn native_vortex_plan_executes_after_planning_runtime_stops() {
    let io_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let (fixture, provider) = io_runtime.block_on(async {
        let fixture = flat_vortex_fixture().await;
        let provider = Arc::new(
            IcebergTableProvider::try_new(fixture.table.clone())
                .unwrap()
                .with_exact_filter_pushdown(true),
        );
        (fixture, provider)
    });
    let planning_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (plan, task_context) = planning_runtime.block_on(async {
        let context = fixture.context();
        context.register_table("ice", provider.clone()).unwrap();
        let plan = context
            .sql("SELECT id FROM ice WHERE id BETWEEN 17 AND 142 ORDER BY id")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();
        assert!(display.contains("VortexScanSource"), "{display}");
        (plan, context.task_ctx())
    });
    drop(planning_runtime);
    let execution_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    execution_runtime.block_on(async {
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            collect(plan, task_context),
        )
        .await
        .unwrap()
        .unwrap();
        let ids = |batches: &[RecordBatch]| {
            batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&batches), (17..=142).collect::<Vec<_>>());

        let context = fixture.context();
        context.register_table("ice", provider.clone()).unwrap();
        let plan = context
            .sql("SELECT id FROM ice WHERE id BETWEEN 17 AND 142 ORDER BY id")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let display = displayable(plan.as_ref()).indent(true).to_string();
        assert!(display.contains("VortexScanSource"), "{display}");
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            collect(plan, context.task_ctx()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ids(&batches), (17..=142).collect::<Vec<_>>());
    });
}

#[tokio::test]
async fn parquet_deletes_keep_dropped_key_columns() {
    assert_deletes_keep_dropped_key_columns(DataFileFormat::Parquet).await;
}

#[tokio::test]
async fn vortex_deletes_keep_dropped_key_columns() {
    assert_deletes_keep_dropped_key_columns(DataFileFormat::Vortex).await;
}

async fn assert_deletes_keep_dropped_key_columns(format: DataFileFormat) {
    let mut fixture = Fixture::new_with_format(true, format).await;
    fixture.add_deletes().await;
    let key_id = fixture
        .table
        .metadata()
        .current_schema()
        .field_by_name("id")
        .unwrap()
        .id;
    let collision = format!("__coldfusion_delete_key_{key_id}");
    let fields = fixture
        .table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .filter(|field| field.name != "id")
        .map(|field| {
            let mut field = field.as_ref().clone();
            if field.name == "label" {
                field.name = collision.clone();
            }
            Arc::new(field)
        })
        .collect();
    fixture.replace_schema_fields(fields);
    let context = fixture.context();
    let provider = Arc::new(IcebergTableProvider::try_new(fixture.table.clone()).unwrap());
    let plan = provider
        .scan(&context.state(), None, &[], None)
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema() == provider.schema())
    );
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize - 7
    );
    context.register_table("ice", provider).unwrap();
    let removed = [0, 31, 32, 97, 128, 255, 256];
    let mask = BooleanArray::from(
        (0..ROWS)
            .map(|row| !removed.contains(&row))
            .collect::<Vec<_>>(),
    );
    let expected = datafusion::arrow::compute::filter_record_batch(&fixture.batch, &mask).unwrap();
    context
        .register_table(
            "expected",
            Arc::new(MemTable::try_new(expected.schema(), vec![vec![expected]]).unwrap()),
        )
        .unwrap();
    let actual = context
        .sql(&format!(
            "SELECT {collision} AS label FROM ice ORDER BY label"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = context
        .sql("SELECT label FROM expected ORDER BY label")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        pretty_format_batches(&actual).unwrap().to_string(),
        pretty_format_batches(&expected).unwrap().to_string()
    );
}

#[tokio::test]
async fn parquet_list_struct_renames_keep_values() {
    assert_list_struct_renames_keep_values(DataFileFormat::Parquet).await;
}

#[tokio::test]
async fn vortex_list_struct_renames_keep_values() {
    assert_list_struct_renames_keep_values(DataFileFormat::Vortex).await;
}

async fn assert_list_struct_renames_keep_values(format: DataFileFormat) {
    use datafusion::arrow::array::{Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use iceberg::spec::StructType;
    let mut fixture = Fixture::new(false).await;
    let value = Arc::new(Field::new("value", DataType::Int32, true));
    let values = StructArray::new(
        vec![value.clone()].into(),
        vec![Arc::new(Int32Array::from_iter_values(0..ROWS as i32))],
        None,
    );
    let element = Arc::new(Field::new("element", values.data_type().clone(), true));
    let list = ListArray::new(
        element,
        OffsetBuffer::new((0..=ROWS as i32).collect::<Vec<_>>().into()),
        Arc::new(values),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![("items", Arc::new(list) as ArrayRef)]).unwrap();
    fixture.table = fixture
        .catalog
        .create_table(
            &NamespaceIdent::new("test".to_owned()),
            TableCreation::builder()
                .name("lists".to_owned())
                .schema(arrow_schema_to_schema_auto_assign_ids(&batch.schema()).unwrap())
                .build(),
        )
        .await
        .unwrap();
    let schema =
        Arc::new(schema_to_arrow_schema(fixture.table.metadata().current_schema()).unwrap());
    let DataType::List(element) = schema.field(0).data_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = element.data_type() else {
        unreachable!()
    };
    let values = StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..ROWS as i32))],
        None,
    );
    let list = ListArray::new(
        element.clone(),
        OffsetBuffer::new((0..=ROWS as i32).collect::<Vec<_>>().into()),
        Arc::new(values),
        None,
    );
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
    let path = format!(
        "{}/lists.{}",
        fixture.table.metadata().location(),
        if format == DataFileFormat::Parquet {
            "parquet"
        } else {
            "vortex"
        }
    );
    fixture.write_batch(&path, format, &batch).await;
    fixture.evolve_field(0, |field| {
        let Type::List(list) = field.field_type.as_mut() else {
            unreachable!()
        };
        let Type::Struct(children) = Arc::make_mut(&mut list.element_field).field_type.as_mut()
        else {
            unreachable!()
        };
        let mut fields = children.fields().to_vec();
        Arc::make_mut(&mut fields[0]).name = "renamed".to_owned();
        *children = StructType::new(fields);
    });
    let context = fixture.context();
    context
        .register_table(
            "ice",
            Arc::new(IcebergTableProvider::try_new(fixture.table).unwrap()),
        )
        .unwrap();
    let actual = context
        .sql("SELECT items FROM ice")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values = actual
        .iter()
        .flat_map(|batch| {
            let list = batch
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let values = list
                .values()
                .slice(start, offsets[offsets.len() - 1] as usize - start);
            let values = values.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(values.fields()[0].name(), "renamed");
            values
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, (0..ROWS as i32).collect::<Vec<_>>());
}

#[tokio::test]
async fn parquet_dynamic_hints_preserve_signed_zero_equality() {
    use datafusion::physical_plan::filter::FilterExec;
    let mut fixture = Fixture::new(false).await;
    let mut columns = fixture.batch.columns().to_vec();
    columns[4] = Arc::new(Float64Array::from_iter_values((0..ROWS).map(
        |row| match row {
            0 => -0.0,
            1 => 0.0,
            _ => 1.0,
        },
    )));
    let batch = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
    let path = fixture.data_path.clone();
    fixture
        .write_batch(&path, DataFileFormat::Parquet, &batch)
        .await;
    let context = fixture.context();
    let provider = IcebergTableProvider::try_new(fixture.table).unwrap();
    let plan = provider
        .scan(&context.state(), Some(&vec![4]), &[], None)
        .await
        .unwrap();
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("special_float", 0));
    let live = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![key.clone()],
        Arc::new(PhysicalLiteral::new(ScalarValue::Boolean(Some(true)))),
    ));
    let equals = Arc::new(BinaryExpr::new(
        key,
        Operator::Eq,
        Arc::new(PhysicalLiteral::new(ScalarValue::Float64(Some(0.0)))),
    ));
    let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(live, Operator::And, equals));
    let plan = with_dynamic_filter(plan, expression.clone(), &context);
    let plan = Arc::new(FilterExec::try_new(expression, plan).unwrap());
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    let mut bits = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    bits.sort_unstable();
    assert_eq!(bits, vec![0, (-0.0_f64).to_bits()]);
}

#[tokio::test]
async fn parquet_initial_defaults_keep_decimal_and_uuid_values() {
    use datafusion::arrow::array::{Decimal128Array, FixedSizeBinaryArray};
    use iceberg::spec::PrimitiveLiteral;
    let mut fixture = Fixture::new(true).await;
    let tx = Transaction::new(&fixture.table);
    fixture.table = tx
        .upgrade_table_version()
        .set_format_version(FormatVersion::V3)
        .apply(tx)
        .unwrap()
        .commit(&fixture.catalog)
        .await
        .unwrap();
    let mut fields = fixture
        .table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    let mut decimal = NestedField::optional(
        100,
        "default_decimal",
        Type::Primitive(PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        }),
    );
    decimal.initial_default = Some(Literal::Primitive(PrimitiveLiteral::Int128(125)));
    let mut uuid = NestedField::optional(101, "default_uuid", Type::Primitive(PrimitiveType::Uuid));
    uuid.initial_default = Some(Literal::Primitive(PrimitiveLiteral::UInt128(123)));
    fields.extend([Arc::new(decimal), Arc::new(uuid)]);
    fixture.replace_schema_fields(fields);
    let context = fixture.context();
    let provider = IcebergTableProvider::try_new(fixture.table).unwrap();
    let plan = provider
        .scan(&context.state(), Some(&vec![6, 7]), &[], None)
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize
    );
    for batch in batches {
        let decimals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert!(decimals.iter().all(|value| value == Some(125)));
        let uuids = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(
            uuids
                .iter()
                .all(|value| value == Some(123_u128.to_be_bytes().as_slice()))
        );
    }
}

#[tokio::test]
async fn parquet_fallible_expressions_ignore_deleted_rows() {
    assert_fallible_expressions_ignore_deleted_rows(DataFileFormat::Parquet).await;
}

#[tokio::test]
async fn vortex_fallible_expressions_ignore_deleted_rows() {
    assert_fallible_expressions_ignore_deleted_rows(DataFileFormat::Vortex).await;
}

async fn assert_fallible_expressions_ignore_deleted_rows(format: DataFileFormat) {
    let mut fixture = Fixture::new_with_format(true, format).await;
    fixture.add_deletes().await;
    let context = fixture.context();
    context
        .register_table(
            "ice",
            Arc::new(IcebergTableProvider::try_new(fixture.table).unwrap()),
        )
        .unwrap();
    let batches = context
        .sql("SELECT id FROM ice WHERE 1 / (id - 31) > 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let batches = context
        .sql("SELECT 1 / (id - 31) AS ratio FROM ice")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS as usize - 7
    );
}
