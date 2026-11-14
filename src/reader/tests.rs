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
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use iceberg::Runtime;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::FileIO;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, Literal, NestedField, NestedFieldRef, PrimitiveType,
    Schema as IcebergSchema, SchemaRef, StructType, Type,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use vortex::array::{ArrayRef, VortexSessionExecute};
use vortex::arrow::ExecuteArrowExt;
use vortex::expr::{get_item, root};
use vortex::scan::selection::Selection;
use vortex::scan::{DataSource, ScanRequest};

use super::{FileCache, IcebergSource};

#[tokio::test]
async fn natural_partitions_combine_row_ranges_selections_and_a_shared_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rows.parquet");
    let field = Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        "1".to_owned(),
    )]));
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(
        Int64Array::from_iter_values(0..128),
    )])
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(32))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema.clone(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let size = path.metadata().unwrap().len();
    let schema = Arc::new(arrow_schema_to_schema_auto_assign_ids(&schema).unwrap());
    let task = FileScanTask::builder()
        .with_file_size_in_bytes(size)
        .with_start(0)
        .with_length(size)
        .with_data_file_path(path.to_str().unwrap().to_owned())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema.clone())
        .with_project_field_ids(vec![1])
        .with_case_sensitive(true)
        .build()
        .unwrap();
    let cache = FileCache::new(FileIO::new_with_fs(), Runtime::current())
        .await
        .unwrap();
    let source = IcebergSource::prepare(&cache, schema.clone(), vec![task], None, vec![schema])
        .await
        .unwrap();
    let all = source.scan(ScanRequest::default()).await.unwrap();
    assert_eq!(all.partition_count().into_inner(), Some(4));
    let request = ScanRequest {
        projection: get_item("id", root()),
        partition_range: Some(1..3),
        partition_selection: Selection::IncludeRoaring([1, 2, 3].into_iter().collect()),
        row_range: Some(4..12),
        selection: Selection::IncludeRoaring([4, 7, 20].into_iter().collect()),
        limit: Some(3),
        ..Default::default()
    };
    let arrays: Vec<ArrayRef> = source
        .scan(request)
        .await
        .unwrap()
        .partitions()
        .map(|partition| partition.and_then(|partition| partition.execute()))
        .try_flatten()
        .try_collect()
        .await
        .unwrap();
    let mut values = Vec::new();
    for array in arrays {
        let array = array
            .execute_arrow(None, &mut cache.session().create_execution_ctx())
            .unwrap();
        values.extend_from_slice(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(values, vec![36, 39, 68]);
}

#[tokio::test]
async fn dropped_nested_delete_keys_keep_parent_nulls_and_initial_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let x = Arc::new(NestedField::optional(
        2,
        "x",
        Type::Primitive(PrimitiveType::Long),
    ));
    let current = default_nested_schema(2, vec![x.clone()]);
    let mut key = NestedField::required(3, "k", Type::Primitive(PrimitiveType::Long));
    key.initial_default = Some(Literal::long(7));
    let key = Arc::new(key);
    let historical = default_nested_schema(1, vec![x, key.clone()]);
    let batch = nested_batch(
        &current,
        vec![0, 1],
        Some(NullBuffer::from(vec![false, true])),
    );
    let path = directory.path().join("rows.parquet");
    let size = write_batch_file(&path, &batch);

    let delete_schema = nested_schema(1, vec![key]);
    let deletes = nested_batch(&delete_schema, vec![7], None);
    let delete_path = directory.path().join("deletes.parquet");
    let delete_size = write_batch_file(&delete_path, &deletes);
    let delete = equality_delete(&delete_path, delete_size, vec![3]);
    let task = parquet_task(&path, size, current.clone(), vec![delete]);
    let cache = FileCache::new(FileIO::new_with_fs(), Runtime::current())
        .await
        .unwrap();
    let source = IcebergSource::prepare(&cache, current.clone(), vec![task], None, vec![
        historical, current,
    ])
    .await
    .unwrap();
    let arrays: Vec<ArrayRef> = source
        .scan(ScanRequest::default())
        .await
        .unwrap()
        .partitions()
        .map(|partition| partition.and_then(|partition| partition.execute()))
        .try_flatten()
        .try_collect()
        .await
        .unwrap();
    let mut rows = 0;
    for array in arrays {
        let array = array
            .execute_arrow(None, &mut cache.session().create_execution_ctx())
            .unwrap();
        let batch = array.as_any().downcast_ref::<StructArray>().unwrap();
        let parent = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(parent.fields().len(), 1, "hidden k leaked into output");
        assert_eq!(
            parent.null_count(),
            parent.len(),
            "NULL parent must survive k=7 deletes"
        );
        rows += parent.len();
    }
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn dropped_parent_keeps_the_full_struct_default_for_delete_keys() {
    let id = Arc::new(NestedField::required(
        10,
        "id",
        Type::Primitive(PrimitiveType::Long),
    ));
    let current = Arc::new(
        IcebergSchema::builder()
            .with_schema_id(2)
            .with_fields(vec![id.clone()])
            .build()
            .unwrap(),
    );
    let x = Arc::new(
        NestedField::required(2, "x", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(Literal::long(5)),
    );
    let key = Arc::new(
        NestedField::required(3, "k", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(Literal::long(7)),
    );
    let parent_schema = default_nested_schema(1, vec![x, key]);
    let historical = Arc::new(
        IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![id, parent_schema.field_by_id(1).unwrap().clone()])
            .build()
            .unwrap(),
    );
    assert_eq!(missing_parent_rows(current, historical).await, 0);
}

#[tokio::test]
async fn retained_parent_fills_hidden_child_defaults_from_empty_object() {
    let id = Arc::new(NestedField::required(
        10,
        "id",
        Type::Primitive(PrimitiveType::Long),
    ));
    let x = Arc::new(
        NestedField::required(2, "x", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(Literal::long(5)),
    );
    let key = Arc::new(
        NestedField::required(3, "k", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(Literal::long(7)),
    );
    let current_parent = default_nested_schema(2, vec![x.clone()]);
    let current = Arc::new(
        IcebergSchema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                id.clone(),
                current_parent.field_by_id(1).unwrap().clone(),
            ])
            .build()
            .unwrap(),
    );
    let historical_parent = default_nested_schema(1, vec![x, key]);
    let historical = Arc::new(
        IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![id, historical_parent.field_by_id(1).unwrap().clone()])
            .build()
            .unwrap(),
    );
    assert_eq!(missing_parent_rows(current, historical).await, 0);
}

async fn missing_parent_rows(current: SchemaRef, historical: SchemaRef) -> usize {
    let directory = tempfile::tempdir().unwrap();
    let physical = IcebergSchema::builder()
        .with_fields(vec![current.field_by_id(10).unwrap().clone()])
        .build()
        .unwrap();
    let arrow = Arc::new(schema_to_arrow_schema(&physical).unwrap());
    let batch = RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(vec![99]))]).unwrap();
    let path = directory.path().join("rows.parquet");
    let size = write_batch_file(&path, &batch);
    let delete_schema = nested_schema(1, vec![historical.field_by_id(3).unwrap().clone()]);
    let delete_path = directory.path().join("deletes.parquet");
    let delete_size = write_batch_file(&delete_path, &nested_batch(&delete_schema, vec![7], None));
    let delete = equality_delete(&delete_path, delete_size, vec![3]);
    let task = parquet_task(&path, size, current.clone(), vec![delete]);
    let cache = FileCache::new(FileIO::new_with_fs(), Runtime::current())
        .await
        .unwrap();
    let source = IcebergSource::prepare(&cache, current.clone(), vec![task], None, vec![
        historical, current,
    ])
    .await
    .unwrap();
    let arrays: Vec<ArrayRef> = source
        .scan(ScanRequest::default())
        .await
        .unwrap()
        .partitions()
        .map(|partition| partition.and_then(|partition| partition.execute()))
        .try_flatten()
        .try_collect()
        .await
        .unwrap();
    arrays.iter().map(ArrayRef::len).sum()
}

fn equality_delete(path: &Path, size: u64, ids: Vec<i32>) -> FileScanTaskDeleteFile {
    FileScanTaskDeleteFile::builder()
        .with_file_path(path.to_str().unwrap().to_owned())
        .with_file_size_in_bytes(size)
        .with_file_type(DataContentType::EqualityDeletes)
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(0)
        .with_equality_ids(Some(ids))
        .build()
        .unwrap()
}

fn parquet_task(
    path: &Path,
    size: u64,
    schema: SchemaRef,
    deletes: Vec<FileScanTaskDeleteFile>,
) -> FileScanTask {
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    FileScanTask::builder()
        .with_file_size_in_bytes(size)
        .with_start(0)
        .with_length(size)
        .with_data_file_path(path.to_str().unwrap().to_owned())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema)
        .with_project_field_ids(fields)
        .with_case_sensitive(true)
        .with_deletes(deletes)
        .build()
        .unwrap()
}

fn write_batch_file(path: &Path, batch: &RecordBatch) -> u64 {
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    path.metadata().unwrap().len()
}

fn nested_schema(schema_id: i32, fields: Vec<NestedFieldRef>) -> SchemaRef {
    let parent = NestedField::optional(1, "p", Type::Struct(StructType::new(fields)));
    Arc::new(
        IcebergSchema::builder()
            .with_schema_id(schema_id)
            .with_fields(vec![Arc::new(parent)])
            .build()
            .unwrap(),
    )
}

fn nested_batch(
    schema: &IcebergSchema,
    values: Vec<i64>,
    validity: Option<NullBuffer>,
) -> RecordBatch {
    let arrow = Arc::new(schema_to_arrow_schema(schema).unwrap());
    let DataType::Struct(children) = arrow.field(0).data_type() else {
        unreachable!()
    };
    let values = StructArray::new(
        children.clone(),
        vec![Arc::new(Int64Array::from(values))],
        validity,
    );
    RecordBatch::try_new(arrow, vec![Arc::new(values)]).unwrap()
}

fn default_nested_schema(schema_id: i32, fields: Vec<NestedFieldRef>) -> SchemaRef {
    let schema = nested_schema(schema_id, fields);
    let mut parent = schema.field_by_id(1).unwrap().as_ref().clone();
    parent.initial_default = Literal::try_from_json(json!({}), &parent.field_type).unwrap();
    Arc::new(
        IcebergSchema::builder()
            .with_schema_id(schema_id)
            .with_fields(vec![Arc::new(parent)])
            .build()
            .unwrap(),
    )
}
