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

//! Conservative proofs used to enable speculative execution, never to reject a callback.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};

pub(super) fn infallible(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    infallible_node(expr, schema)
        && expr
            .children()
            .into_iter()
            .all(|child| infallible(child, schema))
}

pub(super) fn infallible_node(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    if expr.is::<Column>()
        || expr.is::<Literal>()
        || expr.is::<NotExpr>()
        || expr.is::<IsNullExpr>()
        || expr.is::<IsNotNullExpr>()
    {
        true
    } else if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        match binary.op() {
            Operator::And | Operator::Or => true,
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => binary
                .left()
                .data_type(schema)
                .ok()
                .zip(binary.right().data_type(schema).ok())
                .is_some_and(|(left, right)| {
                    left == right
                        && (left.is_integer()
                            || left.is_floating()
                            || matches!(
                                left,
                                DataType::Boolean
                                    | DataType::Utf8
                                    | DataType::LargeUtf8
                                    | DataType::Utf8View
                                    | DataType::Binary
                                    | DataType::LargeBinary
                                    | DataType::BinaryView
                            ))
                }),
            _ => false,
        }
    } else {
        false
    }
}

pub(super) fn conditional(expr: &Arc<dyn PhysicalExpr>) -> bool {
    // Unknown custom nodes and UDFs stay opaque. Only these known eager operators
    // permit extracting non-column children into eagerly evaluated Vortex inputs.
    use datafusion::physical_expr::expressions::{CastExpr, NegativeExpr, TryCastExpr};
    !(expr.is::<Column>()
        || expr.is::<Literal>()
        || expr.is::<CastExpr>()
        || expr.is::<TryCastExpr>()
        || expr.is::<NegativeExpr>()
        || expr.is::<NotExpr>()
        || expr.is::<IsNullExpr>()
        || expr.is::<IsNotNullExpr>()
        || expr
            .downcast_ref::<BinaryExpr>()
            .is_some_and(|binary| !matches!(binary.op(), Operator::And | Operator::Or)))
}
