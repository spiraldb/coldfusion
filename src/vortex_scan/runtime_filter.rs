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

//! Query-local bridge for DataFusion's opaque join membership state.
//!
//! Only immutable snapshots are wrapped. Vortex's dynamic-expression source continues
//! watching the live producer and replaces this function when its generation changes.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CaseExpr, Column, Literal};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{DynamicFilterTracking, PhysicalExpr};
use datafusion::physical_plan::joins::{HashExpr, HashTableLookupExpr};
use vortex::array::{ArrayRef, ExecutionCtx};
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::{DType, Nullability, PType};
use vortex::error::{VortexResult, vortex_ensure};
use vortex::expr::{Expression, get_item, root};
use vortex::scalar_fn::{
    Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable, ScalarFnVTableExt,
};

/// Convert a runtime snapshot containing DF's hash/array-map predicate. Keep its entire
/// CASE expression: DF owns the routing seeds and the rules for unbuilt partitions.
pub(super) fn try_convert(
    expression: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Option<Expression>> {
    if datafusion::physical_expr_common::physical_expr::is_volatile(expression)
        || DynamicFilterTracking::classify(expression).contains_dynamic_filter()
        || expression.data_type(schema)? != DataType::Boolean
        || !supported(expression, schema)?
        || !expression.exists(|node| Ok(node.is::<HashTableLookupExpr>()))?
    {
        return Ok(None);
    }
    let mut columns = collect_columns(expression).into_iter().collect::<Vec<_>>();
    columns.sort_unstable_by_key(|column| column.index());
    let mut fields = Vec::with_capacity(columns.len());
    for column in &columns {
        let Some(field) = schema.fields().get(column.index()) else {
            return Ok(None);
        };
        if field.name() != column.name() || !field.data_type().is_integer() {
            // The first bridge deliberately covers integer join keys only.
            return Ok(None);
        }
        fields.push(field.clone());
    }
    let expression = expression
        .clone()
        .transform_up(|node| {
            let Some(column) = node.downcast_ref::<Column>() else {
                return Ok(Transformed::no(node));
            };
            let index = columns
                .iter()
                .position(|candidate| candidate == column)
                .ok_or_else(|| {
                    DataFusionError::Internal("runtime filter column was not declared".into())
                })?;
            Ok(Transformed::yes(
                Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
            ))
        })
        .data()?;
    let inputs = columns.iter().map(|column| get_item(column.name(), root()));
    let options = RuntimePredicate {
        expression,
        schema: Arc::new(Schema::new(fields)),
    };
    Expression::try_new(DataFusionMembership.bind(options), inputs)
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

// Deliberately accept DF's generated membership/routing grammar, rather than arbitrary
// DF callbacks. Every accepted operation is value-infallible and can be evaluated over
// dictionary values that do not occur in any input row. This makes the bridge exact
// even if presented as a static predicate or projection instead of a join hint.
fn supported(expression: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<bool> {
    let dtype = expression.data_type(schema)?;
    let accepted = if expression.is::<Column>() || expression.is::<Literal>() {
        dtype.is_integer() || dtype == DataType::Boolean
    } else if expression.is::<HashExpr>() || expression.is::<HashTableLookupExpr>() {
        !expression.children().is_empty()
            && (!expression.is::<HashTableLookupExpr>() || expression.children().len() == 1)
            && expression.children().iter().all(|child| {
                child
                    .data_type(schema)
                    .is_ok_and(|dtype| dtype.is_integer())
            })
    } else if let Some(case) = expression.downcast_ref::<CaseExpr>() {
        let condition_type = case
            .expr()
            .map(|base| base.data_type(schema))
            .transpose()?
            .unwrap_or(DataType::Boolean);
        let mut types_match = case
            .else_expr()
            .map(|value| value.data_type(schema))
            .transpose()?
            .is_none_or(|value_type| value_type == dtype);
        for (when, then) in case.when_then_expr() {
            types_match &=
                when.data_type(schema)? == condition_type && then.data_type(schema)? == dtype;
        }
        types_match
    } else if let Some(binary) = expression.downcast_ref::<BinaryExpr>() {
        let left = binary.left().data_type(schema)?;
        let right = binary.right().data_type(schema)?;
        match binary.op() {
            Operator::And | Operator::Or => left == DataType::Boolean && right == DataType::Boolean,
            Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq =>
                left == right && (left.is_integer() || left == DataType::Boolean),
            Operator::Modulo => left == DataType::UInt64 && right == left
                && binary.right().downcast_ref::<Literal>().is_some_and(|literal|
                    matches!(literal.value(), datafusion::scalar::ScalarValue::UInt64(Some(value)) if *value != 0)),
            _ => false,
        }
    } else {
        false
    };
    if !accepted {
        return Ok(false);
    }
    for child in expression.children() {
        if !supported(child, schema)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn integer_type(dtype: &DataType) -> Option<PType> {
    Some(match dtype {
        DataType::Int8 => PType::I8,
        DataType::Int16 => PType::I16,
        DataType::Int32 => PType::I32,
        DataType::Int64 => PType::I64,
        DataType::UInt8 => PType::U8,
        DataType::UInt16 => PType::U16,
        DataType::UInt32 => PType::U32,
        DataType::UInt64 => PType::U64,
        _ => return None,
    })
}

#[derive(Clone, Debug)]
struct RuntimePredicate {
    expression: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
}

impl PartialEq for RuntimePredicate {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.expression, &other.expression) && self.schema == other.schema
    }
}
impl Eq for RuntimePredicate {}
impl Hash for RuntimePredicate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.expression) as *const ()).hash(state);
        self.schema.hash(state);
    }
}
impl fmt::Display for RuntimePredicate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expression.fmt(formatter)
    }
}

#[derive(Clone)]
struct DataFusionMembership;

impl ScalarFnVTable for DataFusionMembership {
    type Options = RuntimePredicate;

    fn id(&self) -> ScalarFnId {
        ScalarFnId::new_static("coldfusion.datafusion_membership")
    }
    // Default serialize returns None: the join's Arc<Map> is local to this execution.
    fn arity(&self, options: &Self::Options) -> Arity {
        Arity::Exact(options.schema.fields().len())
    }
    fn child_name(&self, options: &Self::Options, index: usize) -> ChildName {
        ChildName::new_arc(options.schema.field(index).name().clone().into())
    }
    fn return_dtype(&self, options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
        vortex_ensure!(
            args.len() == options.schema.fields().len(),
            "membership arity mismatch"
        );
        for (dtype, field) in args.iter().zip(options.schema.fields()) {
            vortex_ensure!(
                dtype.is_int()
                    && Some(dtype.as_ptype()) == integer_type(field.data_type())
                    && (!dtype.is_nullable() || field.is_nullable()),
                "membership key must match its logical integer type: {field}, got {dtype}"
            );
        }
        Ok(DType::Bool(Nullability::Nullable))
    }
    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let session = ctx.session().clone();
        let arrow = session.arrow();
        let columns = options
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| arrow.execute_arrow(args.get(index)?, Some(field), ctx))
            .collect::<VortexResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            options.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(args.row_count())),
        )?;
        let result = options
            .expression
            .evaluate(&batch)
            .and_then(|value| value.into_array_of_size(args.row_count()))
            .map_err(|error| {
                vortex::error::vortex_err!("DataFusion membership evaluation failed: {error}")
            })?;
        vortex_ensure!(
            result.data_type() == &DataType::Boolean,
            "membership result must be Boolean"
        );
        arrow.from_arrow_array(result, &Field::new("membership", DataType::Boolean, true))
    }
    fn is_infallible(&self, _options: &Self::Options) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        ArrayRef as ArrowArrayRef, BooleanArray, Int64Array, StringArray,
    };
    use datafusion::common::ScalarValue;
    use datafusion::common::hash_utils::create_hashes;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, Literal,
    };
    use datafusion::physical_plan::joins::join_hash_map::JoinHashMapU32;
    use datafusion::physical_plan::joins::utils::JoinHashMapType;
    use datafusion::physical_plan::joins::{HashExpr, Map, SeededRandomState};
    use vortex::VortexSessionDefault;
    use vortex::array::VortexSessionExecute;
    use vortex::session::VortexSession;
    use vortex_datafusion::convert::ExpressionConverter;

    use super::*;
    use crate::vortex_scan::convert::ConservativeExpressionConverter;

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
        session
            .arrow()
            .execute_arrow(
                result,
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

    #[test]
    fn membership_leaves_live_filters_to_the_generation_watcher() {
        let batch = input();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("id", 1))],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        let live: Arc<dyn PhysicalExpr> = dynamic.clone();
        dynamic.update(lookup(vec![Some(2)], 1, 5)).unwrap();
        assert!(try_convert(&live, &batch.schema()).unwrap().is_none());
        let converter = ConservativeExpressionConverter::new(VortexSession::default());
        for keys in [vec![Some(2)], vec![Some(5)]] {
            let snapshot = lookup(keys, 1, 5);
            dynamic.update(snapshot.clone()).unwrap();
            let snapshot = datafusion::physical_expr_common::physical_expr::snapshot_physical_expr(
                live.clone(),
            )
            .unwrap();
            let converted = converter
                .try_convert(&snapshot, &batch.schema())
                .unwrap()
                .unwrap();
            assert_eq!(
                evaluate(&converted, &batch).to_data(),
                snapshot
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap()
                    .to_data()
            );
        }
        let result = evaluate(
            &converter
                .try_convert(&lookup(vec![Some(5)], 1, 5), &batch.schema())
                .unwrap()
                .unwrap(),
            &batch,
        );
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .true_count(),
            1
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
            .with_expression_converter(Arc::new(ConservativeExpressionConverter::new(session)))
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

    #[test]
    fn membership_declines_fallible_siblings() {
        let batch = input();
        let division: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 1)),
            Operator::Divide,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        ));
        let condition: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            division,
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        ));
        let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            condition,
            Operator::And,
            lookup(vec![Some(2)], 1, 7),
        ));
        assert!(expression.evaluate(&batch).is_err());
        assert!(try_convert(&expression, &batch.schema()).unwrap().is_none());
    }
    #[test]
    fn membership_rejects_malformed_cases_empty_keys_and_narrowing() {
        let batch = input();
        let lookup = lookup(vec![Some(2)], 1, 7);
        let truth: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        for case in [
            CaseExpr::try_new(
                None,
                vec![(one.clone(), lookup.clone())],
                Some(truth.clone()),
            ),
            CaseExpr::try_new(
                Some(one.clone()),
                vec![(truth.clone(), lookup.clone())],
                Some(truth.clone()),
            ),
            CaseExpr::try_new(
                None,
                vec![(truth.clone(), lookup.clone())],
                Some(one.clone()),
            ),
        ] {
            let case: Arc<dyn PhysicalExpr> = Arc::new(case.unwrap());
            assert!(try_convert(&case, &batch.schema()).unwrap().is_none());
        }
        let empty: Arc<dyn PhysicalExpr> = Arc::new(HashExpr::new(
            vec![],
            SeededRandomState::with_seed(0),
            "empty".into(),
        ));
        assert!(!supported(&empty, &batch.schema()).unwrap());
        let converted = try_convert(&lookup, &batch.schema()).unwrap().unwrap();
        let scalar = converted.as_scalar().unwrap();
        assert!(
            scalar
                .return_dtype(&[DType::Primitive(PType::I32, Nullability::Nullable)])
                .is_err()
        );
        let required = RuntimePredicate {
            expression: lookup,
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        };
        assert!(
            DataFusionMembership
                .return_dtype(&required, &[DType::Primitive(
                    PType::I64,
                    Nullability::Nullable
                )])
                .is_err()
        );
    }
}
