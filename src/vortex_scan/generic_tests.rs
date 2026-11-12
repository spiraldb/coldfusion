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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef as ArrowArrayRef, BooleanArray, Int64Array, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::common::hash_utils::create_hashes;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CaseExpr, Column, DynamicFilterPhysicalExpr, Literal,
    };
    use datafusion::physical_plan::joins::join_hash_map::JoinHashMapU32;
    use datafusion::physical_plan::joins::utils::JoinHashMapType;
    use datafusion::physical_plan::joins::{HashExpr, HashTableLookupExpr, Map, SeededRandomState};
    use vortex::VortexSessionDefault;
    use vortex::array::VortexSessionExecute;
    use vortex::arrow::{ArrowSessionExt, ExecuteArrowExt};
    use vortex::expr::Expression;
    use vortex::session::VortexSession;
    use vortex_datafusion::convert::ExpressionConverter;

    use super::super::df_scalar::Mode;
    fn try_convert(
        expr: &Arc<dyn PhysicalExpr>,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> datafusion::common::Result<Option<Expression>> {
        ScanExpressionConverter::new(VortexSession::default()).try_convert(expr, schema)
    }
    use crate::vortex_scan::convert::ScanExpressionConverter;

    fn lookup(keys: Vec<Option<i64>>, column: usize, seed: u64) -> Arc<dyn PhysicalExpr> {
        let state = SeededRandomState::with_seed(seed);
        let values: ArrowArrayRef = Arc::new(Int64Array::from(keys));
        let mut hashes = vec![0; values.len()];
        create_hashes(&[values], state.random_state(), &mut hashes).unwrap();
        let mut map = JoinHashMapU32::with_capacity(hashes.len());
        map.update_from_iter(Box::new(hashes.iter().enumerate()), 0);
        Arc::new(HashTableLookupExpr::new(
            vec![Arc::new(Column::new("id", column))],
            state,
            Arc::new(Map::HashMap(Box::new(map))),
            "test_lookup".into(),
        ))
    }

    fn input() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "unused",
                Arc::new(StringArray::from(vec!["payload"; 6])) as ArrowArrayRef,
            ),
            (
                "id",
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    None,
                    Some(3),
                    Some(4),
                    Some(5),
                ])) as ArrowArrayRef,
            ),
        ])
        .unwrap()
    }

    fn evaluate(expression: &Expression, batch: &RecordBatch) -> ArrowArrayRef {
        let session = VortexSession::default();
        let array = session
            .arrow()
            .from_arrow_record_batch(batch.clone(), &batch.schema())
            .unwrap();
        let result = array.apply(expression).unwrap();
        result
            .execute_arrow(
                Some(&Field::new("result", DataType::Boolean, true)),
                &mut session.create_execution_ctx(),
            )
            .unwrap()
    }

    #[test]
    fn membership_preserves_null_hashing_and_projects_only_keys() {
        let batch = input();
        for keys in [vec![Some(2), Some(5)], vec![Some(2), None]] {
            let predicate = lookup(keys, 1, 17);
            let converted = try_convert(&predicate, &batch.schema()).unwrap().unwrap();
            assert_eq!(converted.children().len(), 1);
            assert!(
                converted
                    .as_scalar()
                    .unwrap()
                    .options()
                    .serialize()
                    .unwrap()
                    .is_none()
            );
            let actual = evaluate(&converted, &batch);
            let expected = predicate
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            assert_eq!(&actual, &expected);
        }
    }

    #[test]
    fn membership_keeps_partition_case_and_unbuilt_partition_fallback() {
        let batch = input();
        let route: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(HashExpr::new(
                vec![Arc::new(Column::new("id", 1))],
                SeededRandomState::with_seed(0),
                "route".into(),
            )),
            Operator::Modulo,
            Arc::new(Literal::new(ScalarValue::UInt64(Some(4)))),
        ));
        let routes = route
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let partition = routes
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap()
            .value(0);
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(route),
                vec![(
                    Arc::new(Literal::new(ScalarValue::UInt64(Some(partition)))),
                    lookup(vec![Some(2), Some(5)], 1, 42),
                )],
                Some(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))),
            )
            .unwrap(),
        );
        let converted = try_convert(&predicate, &batch.schema()).unwrap().unwrap();
        let actual = evaluate(&converted, &batch);
        let mask = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(mask.true_count() > 0 && mask.false_count() > 0);
        assert_eq!(
            actual.to_data(),
            predicate
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
                .to_data()
        );
    }

    #[tokio::test]
    async fn membership_source_observes_updates_and_rejects_rows_before_output() {
        use datafusion::common::config::ConfigOptions;
        use datafusion::datasource::source::{DataSource, DataSourceExec};
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use vortex::buffer::ByteBufferMut;
        use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
        use vortex_datafusion::v2::VortexDataSource;

        let session = VortexSession::default();
        let batch = input();
        let array = session
            .arrow()
            .from_arrow_record_batch(batch.clone(), &batch.schema())
            .unwrap();
        let mut buffer = ByteBufferMut::empty();
        session
            .write_options()
            .write(&mut buffer, array.to_array_stream())
            .await
            .unwrap();
        let file = session.open_options().open_buffer(buffer).unwrap();
        let source = VortexDataSource::builder(file.data_source().unwrap(), session.clone())
            .with_arrow_schema(batch.schema())
            .with_expression_converter(Arc::new(ScanExpressionConverter::new(session)))
            .build()
            .await
            .unwrap();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("id", 1))],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        let pushed = source
            .try_pushdown_filters(
                vec![dynamic.clone() as Arc<dyn PhysicalExpr>],
                &ConfigOptions::new(),
            )
            .unwrap();
        let source = pushed.updated_node.unwrap();
        let context = SessionContext::new();
        // Reuse the planned source; an initial TRUE must not be frozen at planning.
        for key in [2, 5] {
            dynamic.update(lookup(vec![Some(key)], 1, 9)).unwrap();
            let result = collect(
                Arc::new(DataSourceExec::new(source.clone())),
                context.task_ctx(),
            )
            .await
            .unwrap();
            assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let ids = result
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, vec![key]);
        }
        use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
        let projected = source
            .try_swapping_with_projection(&ProjectionExprs::from(vec![ProjectionExpr::new(
                binary(
                    Arc::new(Column::new("id", 1)),
                    Operator::Plus,
                    integer(i64::MAX),
                ),
                "adjusted",
            )]))
            .unwrap()
            .unwrap();
        let batches = collect(Arc::new(DataSourceExec::new(projected)), context.task_ctx())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MAX.wrapping_add(5)
        );
    }
    #[test]
    fn composite_hash_membership_has_no_single_key_restriction() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a",
                Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrowArrayRef,
            ),
            (
                "b",
                Arc::new(StringArray::from(vec!["one", "two", "one"])) as ArrowArrayRef,
            ),
        ])
        .unwrap();
        let state = SeededRandomState::with_seed(19);
        let mut hashes = vec![0];
        create_hashes(
            &[
                Arc::new(Int64Array::from(vec![1])) as ArrowArrayRef,
                Arc::new(StringArray::from(vec!["two"])) as ArrowArrayRef,
            ],
            state.random_state(),
            &mut hashes,
        )
        .unwrap();
        let mut map = JoinHashMapU32::with_capacity(1);
        map.update_from_iter(Box::new(hashes.iter().enumerate()), 0);
        let expression: Arc<dyn PhysicalExpr> = Arc::new(HashTableLookupExpr::new(
            vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))],
            state,
            Arc::new(Map::HashMap(Box::new(map))),
            "composite".into(),
        ));
        let converted = try_convert(&expression, &batch.schema()).unwrap().unwrap();
        let actual = evaluate(&converted, &batch);
        assert_eq!(
            actual
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .true_count(),
            1
        );
        assert_eq!(
            actual.to_data(),
            expression
                .evaluate(&batch)
                .unwrap()
                .into_array_of_size(3)
                .unwrap()
                .to_data()
        );
    }
    #[test]
    fn nonnull_target_field_does_not_discard_input_nulls() {
        use datafusion::physical_expr::expressions::CastExpr;
        let batch = RecordBatch::try_from_iter(vec![(
            "text",
            Arc::new(StringArray::from(vec![Some("1"), None])) as ArrowArrayRef,
        )])
        .unwrap();
        let expression: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new_with_target_field(
            Arc::new(Column::new("text", 0)),
            Arc::new(Field::new("converted", DataType::Int64, false)),
            None,
        ));
        let converted = try_convert(&expression, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            result(&converted, &batch, DataType::Int64).to_data(),
            expression
                .evaluate(&batch)
                .unwrap()
                .into_array_of_size(2)
                .unwrap()
                .to_data()
        );
    }
    #[test]
    fn eager_parent_extracts_native_widening_but_keeps_wrapping_add_in_df() {
        use datafusion::arrow::array::Int32Array;
        use datafusion::physical_expr::expressions::CastExpr;
        let batch = RecordBatch::try_from_iter(vec![(
            "small",
            Arc::new(Int32Array::from(vec![1, 2])) as ArrowArrayRef,
        )])
        .unwrap();
        let cast: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(Column::new("small", 0)),
            DataType::Int64,
            None,
        ));
        let expression = binary(cast, Operator::Plus, integer(i64::MAX));
        let converted = try_convert(&expression, &batch.schema()).unwrap().unwrap();
        assert!(
            converted
                .children()
                .iter()
                .any(|child| child.as_scalar().is_some())
        );
        assert_eq!(
            result(&converted, &batch, DataType::Int64).to_data(),
            expression
                .evaluate(&batch)
                .unwrap()
                .into_array_of_size(2)
                .unwrap()
                .to_data()
        );
    }
    #[test]
    fn membership_rejects_volatile_siblings() {
        use datafusion::common::config::ConfigOptions;
        use datafusion::physical_expr::ScalarFunctionExpr;
        let batch = input();
        let random: Arc<dyn PhysicalExpr> = Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(datafusion::logical_expr::create_udf(
                    "volatile",
                    vec![],
                    DataType::Float64,
                    datafusion::logical_expr::Volatility::Volatile,
                    Arc::new(|_| {
                        Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                            ScalarValue::Float64(Some(0.5)),
                        ))
                    }),
                )),
                vec![],
                &batch.schema(),
                Arc::new(ConfigOptions::new()),
            )
            .unwrap(),
        );
        let condition: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            random,
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.5)))),
        ));
        let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            condition,
            Operator::And,
            lookup(vec![Some(2)], 1, 7),
        ));
        assert!(try_convert(&expression, &batch.schema()).unwrap().is_none());
    }

    fn result(expr: &Expression, batch: &RecordBatch, dtype: DataType) -> ArrowArrayRef {
        let session = VortexSession::default();
        let array = session
            .arrow()
            .from_arrow_record_batch(batch.clone(), &batch.schema())
            .unwrap();
        array
            .apply(expr)
            .unwrap()
            .execute_arrow(
                Some(&Field::new("result", dtype, true)),
                &mut session.create_execution_ctx(),
            )
            .unwrap()
    }
    fn integer(value: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int64(Some(value))))
    }
    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }
    #[test]
    fn generic_projection_preserves_wrapping_and_nulls() {
        let batch = input();
        let expr = binary(
            Arc::new(Column::new("id", 1)),
            Operator::Plus,
            integer(i64::MAX),
        );
        let converted = try_convert(&expr, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            result(&converted, &batch, DataType::Int64).to_data(),
            expr.evaluate(&batch)
                .unwrap()
                .into_array_of_size(batch.num_rows())
                .unwrap()
                .to_data()
        );
        assert!(!converted.as_scalar().unwrap().signature().is_infallible());
    }
    #[test]
    fn conditional_case_keeps_unselected_errors_unevaluated() {
        let batch = input();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(Arc::new(Column::new("id", 1))),
                vec![(integer(2), integer(7))],
                Some(binary(
                    integer(1),
                    Operator::Divide,
                    binary(Arc::new(Column::new("id", 1)), Operator::Minus, integer(2)),
                )),
            )
            .unwrap(),
        );
        let converted = try_convert(&expr, &batch.schema()).unwrap().unwrap();
        assert_eq!(
            result(&converted, &batch, DataType::Int64).to_data(),
            expr.evaluate(&batch)
                .unwrap()
                .into_array_of_size(batch.num_rows())
                .unwrap()
                .to_data()
        );
    }
    #[test]
    fn advisory_fallible_filter_is_conservative_but_exact_filter_errors() {
        let batch = input();
        let expr = binary(
            binary(Arc::new(Column::new("id", 1)), Operator::Divide, integer(0)),
            Operator::Gt,
            integer(0),
        );
        let converter = ScanExpressionConverter::new(VortexSession::default());
        let hint = converter
            .lower(&expr, &batch.schema(), Mode::Hint)
            .unwrap()
            .unwrap();
        assert_eq!(
            evaluate(&hint, &batch)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .true_count(),
            batch.num_rows()
        );
        let exact = converter
            .lower(&expr, &batch.schema(), Mode::Exact)
            .unwrap()
            .unwrap();
        let session = VortexSession::default();
        let array = session
            .arrow()
            .from_arrow_record_batch(batch.clone(), &batch.schema())
            .unwrap();
        let failed = array.apply(&exact).and_then(|array| {
            array.execute_arrow(
                Some(&Field::new("result", DataType::Boolean, true)),
                &mut session.create_execution_ctx(),
            )
        });
        assert!(failed.is_err());
    }
    #[test]
    fn advisory_conjunction_exposes_native_bounds_and_conservatively_relaxes_errors() {
        let batch = input();
        let bound = binary(Arc::new(Column::new("id", 1)), Operator::Gt, integer(2));
        let fallible = binary(
            binary(integer(1), Operator::Divide, integer(0)),
            Operator::Gt,
            integer(0),
        );
        let expression = binary(bound.clone(), Operator::And, fallible);
        let converted = ScanExpressionConverter::new(VortexSession::default())
            .try_convert_dynamic(&expression, &batch.schema())
            .unwrap()
            .unwrap();
        assert_eq!(converted.children().len(), 2); // Native AND, not an opaque DF root.
        assert_eq!(
            evaluate(&converted, &batch).to_data(),
            bound
                .evaluate(&batch)
                .unwrap()
                .into_array_of_size(batch.num_rows())
                .unwrap()
                .to_data()
        );
    }
    #[test]
    fn dynamic_hint_observes_each_generation_and_exact_live_projection_stays_in_df() {
        let batch = input();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("id", 1))],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        let expr: Arc<dyn PhysicalExpr> = dynamic.clone();
        assert!(try_convert(&expr, &batch.schema()).unwrap().is_none());
        let converted = ScanExpressionConverter::new(VortexSession::default())
            .try_convert_dynamic(&expr, &batch.schema())
            .unwrap()
            .unwrap();
        for key in [2, 5] {
            let snapshot = lookup(vec![Some(key)], 1, 17);
            dynamic.update(snapshot.clone()).unwrap();
            assert_eq!(
                evaluate(&converted, &batch).to_data(),
                snapshot
                    .evaluate(&batch)
                    .unwrap()
                    .into_array_of_size(batch.num_rows())
                    .unwrap()
                    .to_data()
            );
        }
    }
    #[test]
    fn unrepresentable_results_stay_in_datafusion() {
        let batch = input();
        let duration: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::DurationNanosecond(Some(1))));
        assert!(try_convert(&duration, &batch.schema()).unwrap().is_none());
        let schema = datafusion::arrow::datatypes::Schema::new(vec![Field::new(
            "decimal",
            DataType::Decimal128(38, 0),
            false,
        )]);
        let expr = binary(
            Arc::new(Column::new("decimal", 0)),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Decimal128(Some(1), 38, 0))),
        );
        assert!(try_convert(&expr, &schema).unwrap().is_none());
    }
}
