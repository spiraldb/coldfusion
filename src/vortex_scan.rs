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

mod convert;

use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::{VortexScanCache, VortexScanSource};
use iceberg::scan::FileScanTask;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::v2::VortexDataSource;

use self::convert::ConservativeExpressionConverter;
use crate::external;

pub(crate) async fn try_scan(
    table: &Table,
    schema: SchemaRef,
    tasks: &[FileScanTask],
    projection: Option<&Vec<usize>>,
    state: &dyn Session,
    cache: &OnceLock<VortexScanCache>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if tasks.is_empty()
        || tasks
            .iter()
            .any(|task| task.data_file_format() != DataFileFormat::Vortex)
    {
        return Ok(None);
    }
    let target = state.config().target_partitions().max(2);
    let cache = cache.get_or_init(|| {
        VortexScanCache::new(
            table.file_io().clone(),
            VortexSession::default(),
            table.runtime().clone(),
        )
    });
    let session = cache.session().clone();
    let Some(source) = VortexScanSource::try_new_with_cache(
        cache,
        table.metadata().current_schema().clone(),
        tasks.to_vec(),
        NonZeroUsize::new(target).expect("scan target has at least two partitions"),
    )
    .await
    .map_err(external)?
    else {
        return Ok(None);
    };
    let source = VortexDataSource::builder(Arc::new(source), session.clone())
        .with_arrow_schema(schema)
        .with_some_projection(projection.cloned())
        .with_expression_converter(Arc::new(ConservativeExpressionConverter::new(session)))
        .build()
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let source = source.repartitioned(target, 0, None)?.ok_or_else(|| {
        DataFusionError::Internal("Vortex source must expose parallel scan partitions".into())
    })?;
    Ok(Some(Arc::new(DataSourceExec::new(source))))
}
