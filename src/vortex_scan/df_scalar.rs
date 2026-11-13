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

//! A typed DataFusion expression embedded in a Vortex expression tree.
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use vortex::array::{ArrayRef, ExecutionCtx};
use vortex::arrow::{ArrowSessionExt, ExecuteArrowExt};
use vortex::dtype::DType;
use vortex::error::{VortexResult, vortex_ensure, vortex_err};
use vortex::expr::{Expression, get_item, root};
use vortex::scalar_fn::{
    Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable, ScalarFnVTableExt,
};

use super::convert::{NativeCandidates, ScanExpressionConverter};
use super::properties;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Mode {
    Exact,
    Hint,
}

pub(super) fn lower(
    converter: &ScanExpressionConverter,
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    mode: Mode,
    candidates: &mut NativeCandidates,
) -> Result<Option<Expression>> {
    let mut output = expr.return_field(schema)?.as_ref().clone();
    let nullable = output.is_nullable() || expr.nullable(schema)?;
    output = output.with_nullable(nullable);
    // Arrow permits decimal arithmetic coefficients outside its declared precision;
    // Vortex's decimal representation does not. Keep such output in DataFusion.
    // Decimal intermediates inside a Boolean/string/etc. callback remain supported.
    if contains_decimal(output.data_type()) {
        return Ok(None);
    }
    let output = Arc::new(output);
    let Ok(dtype) = converter.session.arrow().from_arrow_field(&output) else {
        return Ok(None); // A valid Arrow result need not have a Vortex representation.
    };
    let mut extraction = Extraction {
        converter,
        candidates,
        schema,
        mode,
        inputs: vec![],
        originals: vec![],
        fields: vec![],
    };
    let expression = extraction.rewrite(expr, true, false)?;
    let input_schema = Arc::new(Schema::new(extraction.fields));
    let Ok(input_dtypes) = input_schema
        .fields()
        .iter()
        .map(|field| converter.session.arrow().from_arrow_field(field))
        .collect::<VortexResult<Vec<_>>>()
    else {
        return Ok(None);
    };
    let infallible = mode == Mode::Hint || properties::infallible(&expression, &input_schema);
    let options = Options {
        expression,
        schema: input_schema,
        output,
        dtype,
        input_dtypes,
        infallible,
        mode,
    };
    Expression::try_new(DFScalarFn.bind(options), extraction.inputs)
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

fn contains_decimal(dtype: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    match dtype {
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_decimal(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_decimal(field.data_type())),
        DataType::Dictionary(_, value) => contains_decimal(value),
        DataType::RunEndEncoded(_, value) => contains_decimal(value.data_type()),
        _ => false,
    }
}

struct Extraction<'a> {
    converter: &'a ScanExpressionConverter,
    candidates: &'a mut NativeCandidates,
    schema: &'a Schema,
    mode: Mode,
    inputs: Vec<Expression>,
    originals: Vec<Arc<dyn PhysicalExpr>>,
    fields: Vec<FieldRef>,
}
impl Extraction<'_> {
    fn input(
        &mut self,
        original: &Arc<dyn PhysicalExpr>,
        native: Expression,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let index = if let Some(index) = self
            .originals
            .iter()
            .position(|existing| existing.eq(original))
        {
            index
        } else {
            let index = self.inputs.len();
            let field = original.return_field(self.schema)?;
            self.fields.push(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_nullable(field.is_nullable() || original.nullable(self.schema)?)
                    .with_name(format!("__df_arg{index}")),
            ));
            self.inputs.push(native);
            self.originals.push(original.clone());
            index
        };
        Ok(Arc::new(Column::new(self.fields[index].name(), index)))
    }
    fn rewrite(
        &mut self,
        expr: &Arc<dyn PhysicalExpr>,
        allow_native: bool,
        child: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if let Some(column) = expr.downcast_ref::<Column>() {
            if self
                .schema
                .fields()
                .get(column.index())
                .is_none_or(|field| field.name() != column.name())
            {
                return Err(DataFusionError::Plan(format!(
                    "Invalid column reference: {column}"
                )));
            }
            return self.input(expr, get_item(column.name(), root()));
        }
        if expr.is::<Literal>() {
            return Ok(expr.clone());
        }
        if child
            && allow_native
            && let Some(native) = self
                .converter
                .native_with(expr, self.schema, self.candidates)?
            && (self.mode == Mode::Exact || native_infallible(&native))
        {
            return self.input(expr, native);
        }
        let eager = allow_native && !properties::conditional(expr);
        let children = expr
            .children()
            .into_iter()
            .map(|child| self.rewrite(child, eager, true))
            .collect::<Result<Vec<_>>>()?;
        if expr
            .children()
            .iter()
            .zip(&children)
            .all(|(old, new)| Arc::ptr_eq(old, new))
        {
            Ok(expr.clone())
        } else {
            expr.clone().with_new_children(children)
        }
    }
}

pub(super) fn native_infallible(expr: &Expression) -> bool {
    expr.as_scalar()
        .is_none_or(|function| function.signature().is_infallible())
        && expr.children().iter().all(native_infallible)
}

#[derive(Clone, Debug)]
struct Options {
    expression: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
    output: FieldRef,
    dtype: DType,
    input_dtypes: Vec<DType>,
    infallible: bool,
    mode: Mode,
}
impl PartialEq for Options {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.expression, &other.expression)
            && self.schema == other.schema
            && self.output == other.output
            && self.mode == other.mode
    }
}
impl Eq for Options {}
impl Hash for Options {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.expression) as *const ()).hash(state);
        self.schema.hash(state);
        self.output.hash(state);
        self.mode.hash(state);
    }
}
impl fmt::Display for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expression.fmt(f)
    }
}
#[derive(Clone)]
struct DFScalarFn;
impl ScalarFnVTable for DFScalarFn {
    type Options = Options;
    fn id(&self) -> ScalarFnId {
        ScalarFnId::new_static("coldfusion.datafusion_expr")
    }
    fn arity(&self, options: &Options) -> Arity {
        Arity::Exact(options.schema.fields().len())
    }
    fn child_name(&self, options: &Options, index: usize) -> ChildName {
        ChildName::new_arc(options.schema.field(index).name().clone().into())
    }
    fn return_dtype(&self, options: &Options, args: &[DType]) -> VortexResult<DType> {
        vortex_ensure!(
            args.len() == options.input_dtypes.len(),
            "DF expression arity mismatch"
        );
        for (actual, expected) in args.iter().zip(&options.input_dtypes) {
            vortex_ensure!(
                actual.with_nullability(expected.nullability()) == *expected
                    && (!actual.is_nullable() || expected.is_nullable()),
                "DF expression input type mismatch: {actual} / {expected}"
            );
        }
        Ok(options.dtype.clone())
    }
    fn execute(
        &self,
        options: &Options,
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
            .map(|(index, field)| args.get(index)?.execute_arrow(Some(field), ctx))
            .collect::<VortexResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            options.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(args.row_count())),
        )?;
        let result = options
            .expression
            .evaluate(&batch)
            .and_then(|value| value.into_array_of_size(args.row_count()));
        let result = match result {
            Ok(result) => result,
            Err(_) if options.mode == Mode::Hint => {
                Arc::new(BooleanArray::from(vec![true; args.row_count()]))
            }
            Err(error) => return Err(vortex_err!("DataFusion expression failed: {error}")),
        };
        vortex_ensure!(
            result.data_type() == options.output.data_type(),
            "DF expression returned an unexpected Arrow type"
        );
        arrow.from_arrow_array(result, &options.output)
    }
    fn is_infallible(&self, options: &Options) -> bool {
        options.infallible
    }
}
