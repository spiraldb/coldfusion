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

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::{DataSource, OpenArgs};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, Partitioning, SendableRecordBatchStream, SortOrderPushdownResult,
};

/// Keeps the official Vortex adapter's scan outputs parallel across optimizer rewrites.
///
/// The pinned adapter funnels ordered or fetched scans into one output partition. Leave those
/// operations in DataFusion while retaining its projection/filter pushdown and shared scan queue.
#[derive(Debug)]
pub(super) struct ParallelVortexSource {
    inner: Arc<dyn DataSource>,
}

impl ParallelVortexSource {
    pub(super) fn wrap(inner: Arc<dyn DataSource>) -> Result<Arc<dyn DataSource>> {
        if inner.output_partitioning().partition_count() < 2 || inner.fetch().is_some() {
            return Err(DataFusionError::Internal(
                "ColdFusion Vortex scans require at least two output partitions and no source fetch"
                    .into(),
            ));
        }
        Ok(Arc::new(Self { inner }))
    }
}

impl DataSource for ParallelVortexSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.open(partition, context)
    }

    fn create_sibling_state(&self, config: &ConfigOptions) -> Option<Arc<dyn Any + Send + Sync>> {
        self.inner.create_sibling_state(config)
    }

    fn open_with_args(&self, args: OpenArgs) -> Result<SendableRecordBatchStream> {
        self.inner.open_with_args(args)
    }

    fn fmt_as(&self, format: DisplayFormatType, formatter: &mut Formatter) -> fmt::Result {
        self.inner.fmt_as(format, formatter)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        if target_partitions < 2 || output_ordering.is_some() {
            return Ok(None);
        }
        self.inner
            .repartitioned(target_partitions, repartition_file_min_size, None)?
            .map(Self::wrap)
            .transpose()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.inner.eq_properties()
    }

    fn scheduling_type(&self) -> SchedulingType {
        self.inner.scheduling_type()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        self.inner
            .try_swapping_with_projection(projection)?
            .map(Self::wrap)
            .transpose()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let result = self.inner.try_pushdown_filters(filters, config)?;
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node: result.updated_node.map(Self::wrap).transpose()?,
        })
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn apply_expressions(
        &self,
        visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(visitor)
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn DataSource>> {
        Self::wrap(self.inner.with_new_state(state)?).ok()
    }
}
