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
mod df_scalar;
mod dynamic;
#[cfg(test)]
mod generic_tests;
mod parallel;
mod properties;

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::Runtime;
use iceberg::scan::FileScanTask;
use iceberg::spec::DataFileFormat;
use vortex::expr::and_collect;
use vortex_datafusion::convert::ExpressionConverter;
use vortex_datafusion::v2::VortexDataSource;

use self::convert::{DeleteSafeConverter, ScanExpressionConverter};
use self::parallel::ParallelVortexSource;
use crate::reader::{FileCache, IcebergSource};
use crate::{IcebergTableProvider, external};

pub(crate) async fn scan(
    provider: &IcebergTableProvider,
    tasks: &[FileScanTask],
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
    state: &dyn Session,
) -> Result<Arc<dyn ExecutionPlan>> {
    let table = &provider.table;
    let schema = provider.schema.clone();
    let filter_support = provider.supports_filters_pushdown(&filters.iter().collect::<Vec<_>>())?;
    if tasks.iter().any(|task| {
        !matches!(
            task.data_file_format(),
            DataFileFormat::Parquet | DataFileFormat::Vortex
        )
    }) {
        return Err(DataFusionError::NotImplemented(
            "ColdFusion supports Parquet and Vortex data files".into(),
        ));
    }
    let target = state.config().target_partitions().max(2);
    let cache = provider
        .file_cache
        .get_or_try_init(|| async {
            FileCache::new(
                table.file_io().clone(),
                provider
                    .reader_runtime
                    .clone()
                    .unwrap_or_else(Runtime::current),
            )
            .await
        })
        .await
        .map_err(external)?;
    let session = cache.session().clone();
    let converter = ScanExpressionConverter::new(session.clone());
    // Keep optional row-wise CASE/hash work above Parquet scans; native bounds
    // still enter the layouts. Static filters and projections use the full converter.
    let converter = if tasks
        .iter()
        .all(|task| task.data_file_format() == DataFileFormat::Parquet)
    {
        converter.with_native_dynamic_hints()
    } else {
        converter
    };
    let filter_converter: Arc<dyn ExpressionConverter> =
        if tasks.iter().any(|task| !task.deletes().is_empty()) {
            Arc::new(DeleteSafeConverter(converter.clone()))
        } else {
            Arc::new(converter.clone())
        };
    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
    let mut converted = Vec::new();
    for (filter, support) in filters.iter().zip(filter_support) {
        let physical = state.create_physical_expr(filter.clone(), &df_schema)?;
        // The closed Exact language only compares columns and constants, so it
        // cannot fail on deleted rows. Other filters require the physical proof.
        let expression = if support == TableProviderFilterPushDown::Exact {
            converter.try_convert(&physical, &schema)?
        } else {
            filter_converter.try_convert(&physical, &schema)?
        };
        let Some(expression) = expression else {
            if support == TableProviderFilterPushDown::Exact {
                return Err(DataFusionError::Plan(format!(
                    "Cannot execute exact scan filter {filter}"
                )));
            }
            continue;
        };
        converted.push(expression);
    }
    let source = IcebergSource::prepare(
        cache,
        table.metadata().current_schema().clone(),
        tasks.to_vec(),
        and_collect(converted),
        table.metadata().schemas_iter().cloned().collect(),
    )
    .await
    .map_err(external)?;
    let source = VortexDataSource::builder(Arc::new(source), session.clone())
        .with_arrow_schema(schema)
        .with_some_projection(projection.cloned())
        .with_expression_converter(filter_converter)
        .build()
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let source = source.repartitioned(target, 0, None)?.ok_or_else(|| {
        DataFusionError::Internal("Vortex source must expose parallel scan partitions".into())
    })?;
    Ok(Arc::new(DataSourceExec::new(ParallelVortexSource::wrap(
        source,
    )?)))
}
