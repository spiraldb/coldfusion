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

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, InListExpr, NegativeExpr, TryCastExpr,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use vortex::expr::Expression;
use vortex::session::VortexSession;
use vortex_datafusion::convert::{DefaultExpressionConverter, ExpressionConverter};

pub(super) struct ConservativeExpressionConverter(DefaultExpressionConverter);

impl ConservativeExpressionConverter {
    pub(super) fn new(session: VortexSession) -> Self {
        Self(DefaultExpressionConverter::new(session))
    }
}

impl ExpressionConverter for ConservativeExpressionConverter {
    fn try_convert(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Option<Expression>> {
        if !compatible(expr, schema)? {
            return Ok(None);
        }
        self.0.try_convert(expr, schema)
    }
}

fn compatible(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<bool> {
    // Vortex distinguishes signed zeros in comparisons; DataFusion considers them equal.
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && matches!(
            binary.op(),
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
        )
        && (floating_type(&binary.left().data_type(schema)?)
            || floating_type(&binary.right().data_type(schema)?))
    {
        return Ok(false);
    }
    if let Some(in_list) = expr.downcast_ref::<InListExpr>()
        && floating_type(&in_list.expr().data_type(schema)?)
    {
        return Ok(false);
    }
    // Vortex checks integer overflow, while DataFusion's array arithmetic wraps. Decimal
    // add/subtract/multiply are checked in both engines; permit the unclamped same-type subset.
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && matches!(
            binary.op(),
            Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo
                | Operator::IntegerDivide
        )
        && !matches!(
            expr.data_type(schema)?,
            DataType::Float32 | DataType::Float64
        )
        && !compatible_decimal_arithmetic(binary, schema)?
    {
        return Ok(false);
    }
    if expr.is::<NegativeExpr>()
        && !matches!(
            expr.data_type(schema)?,
            DataType::Float32 | DataType::Float64
        )
    {
        return Ok(false);
    }
    // DataFusion extracts a struct child without applying its parent's validity.
    if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>()
        && ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(function).is_some()
    {
        return Ok(false);
    }
    let cast = expr
        .downcast_ref::<CastExpr>()
        .map(|cast| (cast.expr(), cast.cast_type()))
        .or_else(|| {
            expr.downcast_ref::<TryCastExpr>()
                .map(|cast| (cast.expr(), cast.cast_type()))
        });
    if let Some((input, target)) = cast
        && !lossless_cast(&input.data_type(schema)?, target)
    {
        return Ok(false);
    }
    for child in expr.children() {
        if !compatible(child, schema)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn floating_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, values) => floating_type(values),
        data_type => data_type.is_floating(),
    }
}

fn compatible_decimal_arithmetic(binary: &BinaryExpr, schema: &Schema) -> Result<bool> {
    let left = binary.left().data_type(schema)?;
    let DataType::Decimal128(precision, scale) = left else {
        return Ok(false);
    };
    if binary.right().data_type(schema)? != left
        || precision == 0
        || precision > 38
        || scale < 0
        || scale > precision as i8
    {
        return Ok(false);
    }
    // Valid p-digit operands fit these results without rescaling or rounding. Requiring the
    // full derived precision avoids Arrow's Decimal128 precision clamp at 38, where Vortex's
    // wider decimal precision and its logical bounds can have different behavior.
    let (result_precision, result_scale) = match binary.op() {
        Operator::Plus | Operator::Minus => (precision + 1, scale),
        Operator::Multiply => (2 * precision + 1, 2 * scale),
        _ => return Ok(false),
    };
    Ok(result_precision <= 38
        && result_scale <= 38
        && binary.data_type(schema)? == DataType::Decimal128(result_precision, result_scale))
}

fn lossless_cast(source: &DataType, target: &DataType) -> bool {
    use DataType::*;

    source == target
        || matches!(
            (source, target),
            (Int8, Int16 | Int32 | Int64)
                | (Int16, Int32 | Int64)
                | (Int32, Int64)
                | (UInt8, UInt16 | UInt32 | UInt64)
                | (UInt16, UInt32 | UInt64)
                | (UInt32, UInt64)
                | (Float32, Float64)
                | (Utf8, LargeUtf8 | Utf8View)
                | (Binary, LargeBinary | BinaryView)
        )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use vortex::VortexSessionDefault;

    use super::*;

    #[test]
    fn integer_overflow_uses_datafusion_arithmetic() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let maximum: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Int64(Some(i64::MAX))));
        let arithmetic: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(value, Operator::Plus, maximum));
        let converter = ConservativeExpressionConverter::new(VortexSession::default());
        assert!(
            converter
                .try_convert(&arithmetic, &schema)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn decimal_arithmetic_pushdown_requires_matching_unclamped_types() {
        let converter = ConservativeExpressionConverter::new(VortexSession::default());
        for (operator, left, right, expected) in [
            (
                Operator::Plus,
                DataType::Decimal128(37, 2),
                DataType::Decimal128(37, 2),
                true,
            ),
            (
                Operator::Minus,
                DataType::Decimal128(37, 2),
                DataType::Decimal128(37, 2),
                true,
            ),
            (
                Operator::Multiply,
                DataType::Decimal128(18, 2),
                DataType::Decimal128(18, 2),
                true,
            ),
            (
                Operator::Multiply,
                DataType::Decimal128(15, 2),
                DataType::Decimal128(15, 2),
                true,
            ),
            (
                Operator::Plus,
                DataType::Decimal128(37, 37),
                DataType::Decimal128(37, 37),
                true,
            ),
            (
                Operator::Multiply,
                DataType::Decimal128(18, 18),
                DataType::Decimal128(18, 18),
                true,
            ),
            (
                Operator::Plus,
                DataType::Decimal128(38, 2),
                DataType::Decimal128(38, 2),
                false,
            ),
            (
                Operator::Multiply,
                DataType::Decimal128(19, 2),
                DataType::Decimal128(19, 2),
                false,
            ),
            (
                Operator::Multiply,
                DataType::Decimal128(18, 2),
                DataType::Decimal128(17, 2),
                false,
            ),
            (
                Operator::Plus,
                DataType::Decimal128(18, 2),
                DataType::Decimal128(18, 3),
                false,
            ),
            (
                Operator::Multiply,
                DataType::Decimal128(15, -1),
                DataType::Decimal128(15, -1),
                false,
            ),
            (
                Operator::Divide,
                DataType::Decimal128(18, 2),
                DataType::Decimal128(18, 2),
                false,
            ),
        ] {
            let schema = Schema::new(vec![
                Field::new("a", left, true),
                Field::new("b", right, true),
            ]);
            let arithmetic: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                operator,
                Arc::new(Column::new("b", 1)),
            ));
            assert_eq!(
                converter
                    .try_convert(&arithmetic, &schema)
                    .unwrap()
                    .is_some(),
                expected,
                "{arithmetic} with {schema:?}",
            );
        }
    }

    #[test]
    fn decimal_negation_and_precision_or_scale_casts_stay_in_datafusion() {
        let schema = Schema::new(vec![Field::new("value", DataType::Decimal128(18, 2), true)]);
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let converter = ConservativeExpressionConverter::new(VortexSession::default());
        let negative: Arc<dyn PhysicalExpr> = Arc::new(NegativeExpr::new(value.clone()));
        assert!(converter.try_convert(&negative, &schema).unwrap().is_none());
        for target in [
            DataType::Decimal128(19, 2),
            DataType::Decimal128(18, 1),
            DataType::Decimal128(19, 3),
        ] {
            let cast: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(value.clone(), target, None));
            assert!(converter.try_convert(&cast, &schema).unwrap().is_none());
        }
    }

    #[test]
    fn floating_predicates_stay_in_datafusion_while_arithmetic_can_push_down() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let zero: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Float64(Some(0.0))));
        let converter = ConservativeExpressionConverter::new(VortexSession::default());
        for operator in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let predicate: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(value.clone(), operator, zero.clone()));
            assert!(
                converter
                    .try_convert(&predicate, &schema)
                    .unwrap()
                    .is_none()
            );
        }
        for negated in [false, true] {
            let predicate: Arc<dyn PhysicalExpr> = Arc::new(
                InListExpr::try_new(value.clone(), vec![zero.clone()], negated, &schema).unwrap(),
            );
            assert!(
                converter
                    .try_convert(&predicate, &schema)
                    .unwrap()
                    .is_none()
            );
        }
        for operator in [Operator::Plus, Operator::Divide] {
            let arithmetic: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(value.clone(), operator, zero.clone()));
            assert!(
                converter
                    .try_convert(&arithmetic, &schema)
                    .unwrap()
                    .is_some()
            );
        }
    }
}
