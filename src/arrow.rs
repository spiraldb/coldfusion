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

//! Arrow C Data Interface bridge for Iceberg's Arrow 59 boundary.

use std::error::Error as StdError;

#[cfg(test)]
use arrow_schema_iceberg::Schema as IcebergArrowSchema;
use arrow_schema_iceberg::ffi::FFI_ArrowSchema as IcebergFFIArrowSchema;
use datafusion::arrow::array::ffi::FFI_ArrowSchema;
use datafusion::arrow::datatypes::Schema;
#[cfg(test)]
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids as iceberg_arrow_schema_to_schema;
use iceberg::arrow::schema_to_arrow_schema as iceberg_schema_to_arrow_schema;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::{Error, ErrorKind, Result};

pub(crate) fn schema_to_arrow_schema(schema: &IcebergSchema) -> Result<Schema> {
    let schema = iceberg_schema_to_arrow_schema(schema)?;
    let mut schema = IcebergFFIArrowSchema::try_from(&schema).map_err(invalid_arrow)?;
    // SAFETY: both versions implement the stable ArrowSchema ABI. This transfers
    // ownership and clears the old release callback, preserving field metadata.
    let schema = unsafe { FFI_ArrowSchema::from_raw((&raw mut schema).cast()) };
    Schema::try_from(&schema).map_err(invalid_arrow)
}

fn invalid_arrow(error: impl StdError + Send + Sync + 'static) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        "Cannot transfer Arrow data across Iceberg's version boundary",
    )
    .with_source(error)
}

#[cfg(test)]
pub(crate) fn arrow_schema_to_schema_auto_assign_ids(schema: &Schema) -> Result<IcebergSchema> {
    iceberg_arrow_schema_to_schema(&schema_for_iceberg(schema)?)
}

#[cfg(test)]
pub(crate) fn schema_for_iceberg(schema: &Schema) -> Result<IcebergArrowSchema> {
    let mut schema = FFI_ArrowSchema::try_from(schema).map_err(invalid_arrow)?;
    // SAFETY: ownership moves through the stable ArrowSchema ABI, as above.
    let schema = unsafe { IcebergFFIArrowSchema::from_raw((&raw mut schema).cast()) };
    IcebergArrowSchema::try_from(&schema).map_err(invalid_arrow)
}
