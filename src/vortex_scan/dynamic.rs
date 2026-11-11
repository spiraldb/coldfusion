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

//! Refresh dynamic expressions through the same converter as static projections.
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr_common::physical_expr::{
    snapshot_generation, snapshot_physical_expr,
};
use vortex::dtype::{DType, Nullability};
use vortex::error::VortexResult;
use vortex::expr::transform::replace;
use vortex::expr::{BoundExpression, Expression, dynamic_expression, get_item, lit, pack, root};
use vortex::scalar_fn::fns::dynamic_expression::{DynamicExpressionSource, input_field_name};

use super::convert::ScanExpressionConverter;
use super::df_scalar::Mode;

pub(super) fn lower(
    converter: &ScanExpressionConverter,
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    mode: Mode,
) -> Result<Expression> {
    let mut columns = collect_columns(&expr).into_iter().collect::<Vec<_>>();
    columns.sort_unstable_by_key(|column| column.index());
    for column in &columns {
        if schema
            .fields()
            .get(column.index())
            .is_none_or(|field| field.name() != column.name())
        {
            return Err(DataFusionError::Internal(
                "dynamic expression has invalid column references".into(),
            ));
        }
    }
    let inputs = columns
        .iter()
        .map(|column| get_item(column.name(), root()))
        .collect::<Vec<_>>();
    let scope_projection = pack(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name(), get_item(input_field_name(index), root()))),
        Nullability::NonNullable,
    );
    Ok(dynamic_expression(
        Arc::new(Source {
            expression: expr,
            schema: Arc::new(schema.clone()),
            converter: converter.clone(),
            scope_projection,
            mode,
        }),
        inputs,
    ))
}
struct Source {
    expression: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
    converter: ScanExpressionConverter,
    scope_projection: Expression,
    mode: Mode,
}
impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.expression, f)
    }
}
impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.expression, f)
    }
}
impl DynamicExpressionSource for Source {
    fn generation(&self) -> u64 {
        snapshot_generation(&self.expression)
    }
    fn current(&self, scope: &DType) -> VortexResult<BoundExpression> {
        let snapshot = snapshot_physical_expr(self.expression.clone())
            .map_err(|error| vortex::error::vortex_err!("DF snapshot failed: {error}"))?;
        let expression = self
            .converter
            .lower(&snapshot, &self.schema, self.mode)
            .map_err(|error| vortex::error::vortex_err!("DF dynamic conversion failed: {error}"))?
            .or_else(|| (self.mode == Mode::Hint).then(|| lit(true)))
            .ok_or_else(|| {
                vortex::error::vortex_err!("Exact dynamic expression became unsupported")
            })?;
        replace(expression, &root(), self.scope_projection.clone()).bind(scope)
    }
}
