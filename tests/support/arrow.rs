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

//! Test-fixture transport into Iceberg's Arrow 59 writers.

use std::error::Error;
use std::sync::Arc;

use arrow_array_iceberg::ffi::{FFI_ArrowArray, from_ffi};
use arrow_array_iceberg::{RecordBatch as IcebergRecordBatch, RecordBatchOptions, make_array};
use arrow_schema_iceberg::ffi::FFI_ArrowSchema;
use datafusion::arrow::array::ffi::to_ffi;
use datafusion::arrow::record_batch::RecordBatch;

use super::arrow::schema_for_iceberg;

pub(super) fn batch_for_iceberg(
    batch: &RecordBatch,
) -> Result<IcebergRecordBatch, Box<dyn Error + Send + Sync>> {
    let schema = Arc::new(schema_for_iceberg(&batch.schema())?);
    let mut columns = Vec::with_capacity(batch.num_columns());
    for array in batch.columns() {
        let (mut array, mut schema) = to_ffi(&array.to_data())?;
        // SAFETY: the matching exports implement the stable Arrow C Data ABI.
        // Each from_raw transfers ownership and clears the old release callback.
        let (array, schema) = unsafe {
            (
                FFI_ArrowArray::from_raw((&raw mut array).cast()),
                FFI_ArrowSchema::from_raw((&raw mut schema).cast()),
            )
        };
        // SAFETY: the array and schema came from the same valid Arrow array.
        columns.push(make_array(unsafe { from_ffi(array, &schema) }?));
    }
    Ok(IcebergRecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}
