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

mod predicate;
mod scan;
mod statistics;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
/// Iceberg APIs from the revision used by this provider.
pub use iceberg;
use iceberg::arrow::{schema_to_arrow_schema, schema_with_view_types};
use iceberg::table::Table;

/// Exposes an immutable Iceberg snapshot as parallel Parquet and Vortex scans.
///
/// Scans expose at least two partitions, using the session's target when larger.
#[derive(Debug)]
pub struct IcebergTableProvider {
    table: Table,
    schema: SchemaRef,
    view_types: bool,
    vortex_layout_splits: bool,
    exact_filter_pushdown: bool,
    exact_filter_columns: HashSet<String>,
}

impl IcebergTableProvider {
    /// Creates a provider for the current schema and snapshot of `table`.
    pub fn try_new(table: Table) -> Result<Self> {
        Self::try_new_with_view_types(table, false)
    }

    /// Creates a provider with optional top-level string and binary view arrays.
    ///
    /// Nested fields retain their canonical Iceberg Arrow types. Every scan uses
    /// the selected output types for both Parquet and Vortex data files.
    pub fn try_new_with_view_types(table: Table, view_types: bool) -> Result<Self> {
        // Required fields without initial defaults must be physically present in
        // valid Iceberg files. Check history by ID so renames retain eligibility.
        let exact_filter_columns = if table
            .metadata()
            .partition_specs_iter()
            .all(|spec| spec.fields().is_empty())
        {
            table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .filter(|field| {
                    table.metadata().schemas_iter().all(|schema| {
                        schema
                            .as_struct()
                            .field_by_id(field.id)
                            .is_some_and(|historical| {
                                historical.required && historical.initial_default.is_none()
                            })
                    })
                })
                .map(|field| field.name.clone())
                .collect()
        } else {
            HashSet::new()
        };
        let schema = schema_to_arrow_schema(table.metadata().current_schema()).map_err(external)?;
        let schema = Arc::new(if view_types {
            schema_with_view_types(&schema)
        } else {
            schema
        });
        Ok(Self {
            table,
            schema,
            view_types,
            vortex_layout_splits: false,
            exact_filter_pushdown: false,
            exact_filter_columns,
        })
    }

    /// Filters Vortex layout batches before slicing the Arrow output batches.
    pub fn with_vortex_layout_splits(mut self, enabled: bool) -> Self {
        self.vortex_layout_splits = enabled;
        self
    }

    /// Removes residuals for supported predicates on required fields without defaults.
    ///
    /// Partitioned tables retain residuals because readers may synthesize partition
    /// values after filtering. Optional fields also retain residuals: an older file
    /// may omit them, and native readers can conservatively retain those rows.
    /// This policy trusts valid Iceberg files and their field-ID or name mappings.
    pub fn with_exact_filter_pushdown(mut self, enabled: bool) -> Self {
        self.exact_filter_pushdown = enabled;
        self
    }
}

fn external(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };
        let mut builder = self
            .table
            .scan()
            .select(schema.fields().iter().map(|f| f.name()));
        if let Some(predicate) = filters
            .iter()
            .filter_map(|expr| predicate::predicate(expr, self.table.metadata().current_schema()))
            .reduce(|a, b| a.and(b))
        {
            builder = builder.with_filter(predicate);
        }
        let scan = builder.build().map_err(external)?;
        let tasks = futures::TryStreamExt::try_collect::<Vec<_>>(
            scan.plan_files().await.map_err(external)?,
        )
        .await
        .map_err(external)?;
        Ok(Arc::new(
            scan::IcebergScan::try_new(
                self.table.clone(),
                schema,
                tasks,
                state.config().target_partitions(),
                state.config().batch_size(),
                !filters.is_empty(),
                state.config_options().execution.parquet.enable_page_index,
            )?
            .with_view_types(self.view_types)
            .with_vortex_layout_splits(self.vortex_layout_splits),
        ))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if self.exact_filter_pushdown
                    && expr
                        .column_refs()
                        .iter()
                        .all(|column| self.exact_filter_columns.contains(column.name.as_str()))
                    && predicate::exact_predicate(expr, self.table.metadata().current_schema())
                        .is_some()
                {
                    TableProviderFilterPushDown::Exact
                } else if predicate::predicate(expr, self.table.metadata().current_schema())
                    .is_some()
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    fn statistics(&self) -> Option<Statistics> {
        let snapshot = self.table.metadata().current_snapshot()?;
        let summary = &snapshot.summary().additional_properties;
        let mut statistics = Statistics::new_unknown(&self.schema);
        statistics.num_rows = summary
            .get("total-records")
            .and_then(|value| value.parse().ok())
            .map_or(Precision::Absent, Precision::Inexact);
        statistics::output_bytes(&mut statistics, &self.schema);
        Some(statistics)
    }
}
