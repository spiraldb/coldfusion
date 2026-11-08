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

use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema, Type};

pub(crate) fn predicate(expr: &Expr, schema: &Schema) -> Option<Predicate> {
    convert(expr, schema, true)
}

pub(crate) fn exact_predicate(expr: &Expr, schema: &Schema) -> Option<Predicate> {
    if !exact_expression(expr, schema) {
        return None;
    }
    convert(expr, schema, false)
}

fn exact_expression(expr: &Expr, schema: &Schema) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            exact_expression(&binary.left, schema) && exact_expression(&binary.right, schema)
        }
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) =>
        {
            exact_comparison(&binary.left, &binary.right, schema)
                || exact_comparison(&binary.right, &binary.left, schema)
        }
        Expr::Column(_) => matches!(exact_type(expr, schema), Some(PrimitiveType::Boolean)),
        Expr::IsNull(column) | Expr::IsNotNull(column) => exact_type(column, schema).is_some(),
        Expr::Between(between) if !between.negated => {
            exact_comparison(&between.expr, &between.low, schema)
                && exact_comparison(&between.expr, &between.high, schema)
        }
        Expr::InList(list) if !list.negated && !list.list.is_empty() => list
            .list
            .iter()
            .all(|value| exact_comparison(&list.expr, value, schema)),
        Expr::Like(like) if !like.negated => {
            matches!(exact_type(&like.expr, schema), Some(PrimitiveType::String))
        }
        _ => false,
    }
}

fn exact_type<'a>(expr: &Expr, schema: &'a Schema) -> Option<&'a PrimitiveType> {
    let Expr::Column(column) = expr else {
        return None;
    };
    let field = schema
        .as_struct()
        .fields()
        .iter()
        .find(|field| field.name == column.name)?;
    if !field.required || field.initial_default.is_some() {
        return None;
    }
    match field.field_type.as_ref() {
        Type::Primitive(primitive)
            if matches!(
                primitive,
                PrimitiveType::Boolean
                    | PrimitiveType::Int
                    | PrimitiveType::Long
                    | PrimitiveType::Decimal { .. }
                    | PrimitiveType::Date
                    | PrimitiveType::String
            ) =>
        {
            Some(primitive)
        }
        _ => None,
    }
}

fn exact_comparison(column: &Expr, value: &Expr, schema: &Schema) -> bool {
    let Some(primitive) = exact_type(column, schema) else {
        return false;
    };
    let Some(value) = scalar(value) else {
        return false;
    };
    match (primitive, value) {
        (PrimitiveType::Boolean, ScalarValue::Boolean(Some(_)))
        | (PrimitiveType::Int | PrimitiveType::Long, ScalarValue::Int8(Some(_)))
        | (PrimitiveType::Int | PrimitiveType::Long, ScalarValue::Int16(Some(_)))
        | (PrimitiveType::Int | PrimitiveType::Long, ScalarValue::Int32(Some(_)))
        | (PrimitiveType::Long, ScalarValue::Int64(Some(_)))
        | (PrimitiveType::Date, ScalarValue::Date32(Some(_)))
        | (PrimitiveType::String, ScalarValue::Utf8(Some(_)))
        | (PrimitiveType::String, ScalarValue::Utf8View(Some(_)))
        | (PrimitiveType::String, ScalarValue::LargeUtf8(Some(_))) => true,
        (PrimitiveType::Int, ScalarValue::Int64(Some(value))) => i32::try_from(value).is_ok(),
        (
            PrimitiveType::Decimal { precision, scale },
            ScalarValue::Decimal128(Some(value), _, literal_scale),
        ) => {
            i8::try_from(*scale).ok() == Some(literal_scale)
                && value.unsigned_abs() < 10_u128.pow(*precision)
        }
        _ => false,
    }
}

fn convert(expr: &Expr, schema: &Schema, partial_conjunction: bool) -> Option<Predicate> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = convert(&binary.left, schema, partial_conjunction);
            let right = convert(&binary.right, schema, partial_conjunction);
            match (left, right) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (left, right) if partial_conjunction => left.or(right),
                _ => None,
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => Some(
            convert(&binary.left, schema, partial_conjunction)?.or(convert(
                &binary.right,
                schema,
                partial_conjunction,
            )?),
        ),
        Expr::BinaryExpr(binary) => comparison(&binary.left, binary.op, &binary.right, schema),
        // Dropping a conjunct below NOT can discard rows that satisfy the original filter.
        Expr::Not(inner) => Some(!convert(inner, schema, false)?),
        Expr::Column(_) => Some(reference(expr, schema)?.equal_to(Datum::bool(true))),
        Expr::IsNull(inner) => Some(reference(inner, schema)?.is_null()),
        Expr::IsNotNull(inner) => Some(reference(inner, schema)?.is_not_null()),
        Expr::Between(between) => {
            let column = reference(&between.expr, schema)?;
            let low = literal(&between.low)?;
            let high = literal(&between.high)?;
            Some(if between.negated {
                column.clone().less_than(low).or(column.greater_than(high))
            } else {
                column
                    .clone()
                    .greater_than_or_equal_to(low)
                    .and(column.less_than_or_equal_to(high))
            })
        }
        Expr::InList(list) => {
            let column = reference(&list.expr, schema)?;
            let literals = list.list.iter().map(literal).collect::<Option<Vec<_>>>()?;
            Some(if list.negated {
                column.is_not_in(literals)
            } else {
                column.is_in(literals)
            })
        }
        Expr::Like(like) if like.escape_char.is_none() && !like.case_insensitive => {
            let column = reference(&like.expr, schema)?;
            let pattern = scalar(&like.pattern)?;
            let pattern = match &pattern {
                ScalarValue::Utf8(Some(value))
                | ScalarValue::Utf8View(Some(value))
                | ScalarValue::LargeUtf8(Some(value)) => value,
                _ => return None,
            };
            let prefix = pattern.strip_suffix('%')?;
            if prefix.contains(['%', '_', '\\']) {
                return None;
            }
            Some(if like.negated {
                column.not_starts_with(Datum::string(prefix))
            } else {
                column.starts_with(Datum::string(prefix))
            })
        }
        _ => None,
    }
}

fn reference(expr: &Expr, schema: &Schema) -> Option<Reference> {
    match expr {
        Expr::Column(column) => {
            let field = schema
                .as_struct()
                .fields()
                .iter()
                .find(|field| field.name == column.name)?;
            // Physical predicates run before defaults and nested reconstruction. Iceberg's
            // NaN bounds also differ from DataFusion's total-order floating comparisons.
            if !matches!(field.field_type.as_ref(), Type::Primitive(primitive)
                if !matches!(primitive, PrimitiveType::Float | PrimitiveType::Double))
                || field.initial_default.is_some()
            {
                return None;
            }
            Some(Reference::new(&column.name))
        }
        _ => None,
    }
}

fn comparison(left: &Expr, op: Operator, right: &Expr, schema: &Schema) -> Option<Predicate> {
    let (column, value, reverse) = if let Some(column) = reference(left, schema) {
        (column, literal(right)?, false)
    } else {
        (reference(right, schema)?, literal(left)?, true)
    };
    let op = match (op, reverse) {
        (Operator::Eq, _) => PredicateOperator::Eq,
        (Operator::NotEq, _) => PredicateOperator::NotEq,
        (Operator::Lt, false) | (Operator::Gt, true) => PredicateOperator::LessThan,
        (Operator::LtEq, false) | (Operator::GtEq, true) => PredicateOperator::LessThanOrEq,
        (Operator::Gt, false) | (Operator::Lt, true) => PredicateOperator::GreaterThan,
        (Operator::GtEq, false) | (Operator::LtEq, true) => PredicateOperator::GreaterThanOrEq,
        _ => return None,
    };
    Some(Predicate::Binary(BinaryExpression::new(op, column, value)))
}

fn scalar(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => scalar(&cast.expr)?.cast_to(cast.field.data_type()).ok(),
        _ => None,
    }
}

fn literal(expr: &Expr) -> Option<Datum> {
    match scalar(expr)? {
        ScalarValue::Boolean(Some(value)) => Some(Datum::bool(value)),
        ScalarValue::Int8(Some(value)) => Some(Datum::int(i32::from(value))),
        ScalarValue::Int16(Some(value)) => Some(Datum::int(i32::from(value))),
        ScalarValue::Int32(Some(value)) => Some(Datum::int(value)),
        ScalarValue::Int64(Some(value)) => Some(Datum::long(value)),
        ScalarValue::Float32(Some(value)) if value.is_finite() => Some(Datum::float(value)),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Some(Datum::double(value)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(Datum::string(value)),
        ScalarValue::Binary(Some(value))
        | ScalarValue::BinaryView(Some(value))
        | ScalarValue::LargeBinary(Some(value)) => Some(Datum::binary(value)),
        ScalarValue::Date32(Some(value)) => Some(Datum::date(value)),
        ScalarValue::Date64(Some(value)) if value % 86_400_000 == 0 => {
            Some(Datum::date(i32::try_from(value / 86_400_000).ok()?))
        }
        ScalarValue::TimestampMicrosecond(Some(value), timezone) => Some(if timezone.is_some() {
            Datum::timestamptz_micros(value)
        } else {
            Datum::timestamp_micros(value)
        }),
        ScalarValue::TimestampNanosecond(Some(value), timezone) => Some(if timezone.is_some() {
            Datum::timestamptz_nanos(value)
        } else {
            Datum::timestamp_nanos(value)
        }),
        ScalarValue::Decimal128(Some(value), _, scale) if (0..=38).contains(&scale) => {
            Datum::decimal_from_str(format!("{value}e-{scale}")).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::expr::{Between, InList};
    use datafusion::logical_expr::{Expr, cast, col, lit};
    use datafusion::scalar::ScalarValue;
    use iceberg::expr::{Predicate, Reference};
    use iceberg::spec::{Datum, Literal, NestedField, PrimitiveType, Schema, StructType, Type};

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "a", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "b", PrimitiveType::Int.into()).into(),
                NestedField::optional(3, "date", PrimitiveType::Date.into()).into(),
                NestedField::optional(4, "price", Type::decimal(12, 3).unwrap()).into(),
            ])
            .build()
            .unwrap()
    }

    fn predicate(expr: &Expr) -> Option<Predicate> {
        super::predicate(expr, &schema())
    }

    #[test]
    fn unsafe_physical_predicates_keep_safe_conjuncts_only() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "a", PrimitiveType::Int.into()).into(),
                NestedField::optional(2, "defaulted", PrimitiveType::Int.into())
                    .with_initial_default(Literal::int(7))
                    .into(),
                NestedField::optional(
                    3,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(4, "value", PrimitiveType::Int.into()).into(),
                    ])),
                )
                .into(),
                NestedField::optional(5, "floating", PrimitiveType::Double.into()).into(),
            ])
            .build()
            .unwrap();
        let safe = col("a").gt(lit(4_i32));
        for unsupported in [
            col("defaulted").eq(lit(7_i32)),
            col("nested").is_not_null(),
            col("nested.value").is_null(),
            col("floating").gt(lit(1_f64)),
            col("floating").is_not_null(),
        ] {
            assert_eq!(super::predicate(&unsupported, &schema), None);
            assert_eq!(super::exact_predicate(&unsupported, &schema), None);
            assert_eq!(
                super::predicate(&safe.clone().and(unsupported.clone()), &schema),
                Some(Reference::new("a").greater_than(Datum::int(4)))
            );
            assert_eq!(
                super::predicate(&safe.clone().or(unsupported.clone()), &schema),
                None
            );
            assert_eq!(
                super::predicate(&Expr::Not(Box::new(safe.clone().and(unsupported))), &schema),
                None
            );
        }
    }

    #[test]
    fn partial_conjunctions_do_not_escape_disjunction_or_negation() {
        let supported = col("a").gt(lit(4_i32));
        let unsupported = (col("b") + lit(1_i32)).gt(lit(3_i32));
        let expected = Some(Reference::new("a").greater_than(Datum::int(4)));
        assert_eq!(
            predicate(&supported.clone().and(unsupported.clone())),
            expected
        );
        assert_eq!(
            super::exact_predicate(&supported.clone().and(unsupported.clone()), &schema()),
            None
        );
        assert_eq!(predicate(&supported.clone().or(unsupported.clone())), None);
        assert_eq!(
            predicate(&Expr::Not(Box::new(supported.and(unsupported)))),
            None
        );
    }

    #[test]
    fn comparisons_reverse_operands_without_discarding_column_casts() {
        assert_eq!(
            predicate(&lit(7_i64).lt(col("a"))),
            Some(Reference::new("a").greater_than(Datum::long(7)))
        );
        assert_eq!(
            predicate(&cast(col("a"), DataType::Int32).eq(lit(7_i32))),
            None
        );
    }

    #[test]
    fn date_decimal_ranges_preserve_values() {
        let date_range = Expr::Between(Between::new(
            Box::new(col("date")),
            false,
            Box::new(lit(ScalarValue::Date32(Some(-1)))),
            Box::new(lit(ScalarValue::Date32(Some(10)))),
        ));
        assert_eq!(
            predicate(&date_range),
            Some(
                Reference::new("date")
                    .greater_than_or_equal_to(Datum::date(-1))
                    .and(Reference::new("date").less_than_or_equal_to(Datum::date(10)))
            )
        );
        assert_eq!(
            predicate(&col("price").eq(lit(ScalarValue::Decimal128(Some(-12345), 12, 3)))),
            Some(Reference::new("price").equal_to(Datum::decimal_from_str("-12.345").unwrap()))
        );
    }

    #[test]
    fn negated_between_keeps_an_exactness_residual() {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", PrimitiveType::Int.into()).into(),
            ])
            .build()
            .unwrap();
        for negated in [false, true] {
            let range = Expr::Between(Between::new(
                Box::new(col("id")),
                negated,
                Box::new(lit(1_i32)),
                Box::new(lit(10_i32)),
            ));
            assert!(super::predicate(&range, &schema).is_some());
            assert_eq!(super::exact_predicate(&range, &schema).is_some(), !negated);
        }
    }

    #[test]
    fn null_lists_and_unrepresentable_literals_stay_residual() {
        for negated in [false, true] {
            let list = Expr::InList(InList::new(
                Box::new(col("a")),
                vec![lit(1_i32), lit(ScalarValue::Int32(None))],
                negated,
            ));
            assert_eq!(predicate(&list), None);
        }
        for value in [
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Date64(Some(-1)),
            ScalarValue::Decimal128(Some(1), 3, -1),
        ] {
            assert_eq!(predicate(&col("a").eq(lit(value))), None);
        }
    }
}
