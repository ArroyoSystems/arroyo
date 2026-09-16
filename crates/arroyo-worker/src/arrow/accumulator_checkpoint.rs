//! Caller-side adaptation of DataFusion's consuming accumulator state API.
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::{
    average, correlation, count, covariance, regr, stddev, sum, variance,
};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_plan::Accumulator;
use std::sync::Arc;

/// Only audited implementations have complete state for checkpoint -> retract.
/// A function name or supports_retract_batch() is not enough (e.g. MIN/MAX,
/// DISTINCT, and user-defined aggregates need the stored-input path instead).
pub(super) fn state_fields(expr: &AggregateFunctionExpr) -> Result<Option<Vec<FieldRef>>> {
    if expr.is_distinct() || !expr.order_bys().is_empty() {
        return Ok(None);
    }
    let udf = expr.fun().inner();
    let is_sum = udf.downcast_ref::<sum::Sum>().is_some();
    let is_avg = udf.downcast_ref::<average::Avg>().is_some();
    let supported = is_sum
        || is_avg
        || udf.downcast_ref::<count::Count>().is_some()
        || udf.downcast_ref::<variance::VarianceSample>().is_some()
        || udf.downcast_ref::<variance::VariancePopulation>().is_some()
        || udf.downcast_ref::<stddev::Stddev>().is_some()
        || udf.downcast_ref::<stddev::StddevPop>().is_some()
        || udf.downcast_ref::<covariance::CovarianceSample>().is_some()
        || udf
            .downcast_ref::<covariance::CovariancePopulation>()
            .is_some()
        || udf.downcast_ref::<correlation::Correlation>().is_some()
        || udf.downcast_ref::<regr::Regr>().is_some();
    if !supported
        || ((is_sum || is_avg)
            && !matches!(
                expr.field().data_type(),
                DataType::Int64
                    | DataType::UInt64
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Duration(_)
            ))
    {
        return Ok(None);
    }
    let mut fields = expr.state_fields()?;
    if is_sum {
        // Sliding SUM also exports a count, needed to restore the empty/null case.
        fields.push(Arc::new(Field::new(
            format_state_name(expr.name(), "count"),
            DataType::UInt64,
            true,
        )));
    }
    Ok(Some(fields))
}

/// Export exactly once and rebuild, even when state() happens to be non-consuming.
/// Failure must abort the operator: the original accumulator may be drained.
pub(super) fn snapshot(
    expr: &AggregateFunctionExpr,
    fields: &[FieldRef],
    accumulator: &mut Box<dyn Accumulator>,
) -> Result<Vec<ScalarValue>> {
    let mut replacement = expr.create_sliding_accumulator()?;
    let state = accumulator.state()?;
    let arrays = state
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), arrays)?;
    replacement.merge_batch(batch.columns())?;
    *accumulator = replacement;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, Decimal128Array, DurationMicrosecondArray, Float64Array, Int64Array, UInt64Array,
        new_null_array,
    };
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn expression(udf: Arc<AggregateUDF>, values: &[ArrayRef]) -> AggregateFunctionExpr {
        let schema = Arc::new(Schema::new(
            values
                .iter()
                .enumerate()
                .map(|(i, v)| Field::new(format!("v{i}"), v.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let args = (0..values.len())
            .map(|i| col(&format!("v{i}"), &schema).unwrap())
            .collect();
        AggregateExprBuilder::new(udf, args)
            .schema(schema)
            .alias("test")
            .build()
            .unwrap()
    }

    fn assert_value(actual: ScalarValue, expected: ScalarValue) {
        match (&actual, &expected) {
            (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => assert!(
                (a - b).abs() <= 1e-10 * (1.0 + b.abs()) || (a.is_nan() && b.is_nan()),
                "{actual:?} != {expected:?}"
            ),
            _ => assert_eq!(actual, expected),
        }
    }

    fn verify(
        expr: &AggregateFunctionExpr,
        live: &mut Box<dyn Accumulator>,
        control: &mut Box<dyn Accumulator>,
    ) -> Result<()> {
        let expected = control.evaluate()?;
        assert_value(live.evaluate()?, expected.clone());
        let fields = state_fields(expr)?.expect("audited aggregate");
        let first = snapshot(expr, &fields, live)?;
        // Snapshot must leave the live accumulator usable, including repeated snapshots.
        assert_value(live.evaluate()?, expected.clone());
        assert_eq!(first, snapshot(expr, &fields, live)?);
        let mut restored = expr.create_sliding_accumulator()?;
        restored.merge_batch(
            &first
                .iter()
                .map(ScalarValue::to_array)
                .collect::<Result<Vec<_>>>()?,
        )?;
        assert_value(restored.evaluate()?, expected);
        *live = restored;
        Ok(())
    }

    fn continuation(udf: Arc<AggregateUDF>, values: Vec<ArrayRef>) -> Result<()> {
        let expr = expression(udf, &values);
        let mut live = expr.create_sliding_accumulator()?;
        let mut control = expr.create_sliding_accumulator()?;
        verify(&expr, &mut live, &mut control)?;
        let nulls = values
            .iter()
            .map(|v| new_null_array(v.data_type(), 3))
            .collect::<Vec<_>>();
        live.update_batch(&nulls)?;
        control.update_batch(&nulls)?;
        verify(&expr, &mut live, &mut control)?;
        live.retract_batch(&nulls)?;
        control.retract_batch(&nulls)?;
        live.update_batch(&values)?;
        control.update_batch(&values)?;
        verify(&expr, &mut live, &mut control)?;
        // Changelogs retract out of arrival order, unlike a FIFO sliding window.
        for row in [1, 0, 2, 3] {
            let retract = values.iter().map(|v| v.slice(row, 1)).collect::<Vec<_>>();
            live.retract_batch(&retract)?;
            control.retract_batch(&retract)?;
            verify(&expr, &mut live, &mut control)?;
        }
        live.update_batch(&values)?;
        control.update_batch(&values)?;
        verify(&expr, &mut live, &mut control)
    }

    #[test]
    fn sum_and_average_checkpoint_empty_null_duplicate_and_refill() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(10), None])),
            Arc::new(UInt64Array::from(vec![Some(5), Some(5), Some(10), None])),
            Arc::new(Float64Array::from(vec![
                Some(5.0),
                Some(5.0),
                Some(10.0),
                None,
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(500), Some(500), Some(1000), None])
                    .with_precision_and_scale(12, 2)?,
            ),
            Arc::new(DurationMicrosecondArray::from(vec![
                Some(5),
                Some(5),
                Some(10),
                None,
            ])),
        ];
        for values in arrays {
            let expr = expression(sum::sum_udaf(), std::slice::from_ref(&values));
            assert_eq!(expr.state_fields()?.len(), 1);
            assert_eq!(state_fields(&expr)?.unwrap().len(), 2);
            continuation(sum::sum_udaf(), vec![values.clone()])?;
            // Physical AVG receives integers already coerced to Float64 by planning.
            if !matches!(values.data_type(), DataType::Int64 | DataType::UInt64) {
                continuation(average::avg_udaf(), vec![values])?;
            }
        }
        Ok(())
    }

    #[test]
    fn statistical_checkpoints_continue_and_retract() -> Result<()> {
        let x: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(3.0),
            Some(7.0),
            None,
        ]));
        let y: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(4.0),
            Some(9.0),
            Some(6.0),
            None,
        ]));
        for udf in [
            count::count_udaf(),
            variance::var_samp_udaf(),
            variance::var_pop_udaf(),
            stddev::stddev_udaf(),
            stddev::stddev_pop_udaf(),
        ] {
            continuation(udf, vec![x.clone()])?;
        }
        for udf in [
            covariance::covar_samp_udaf(),
            covariance::covar_pop_udaf(),
            correlation::corr_udaf(),
            regr::regr_slope_udaf(),
            regr::regr_intercept_udaf(),
            regr::regr_count_udaf(),
            regr::regr_r2_udaf(),
            regr::regr_avgx_udaf(),
            regr::regr_avgy_udaf(),
            regr::regr_sxx_udaf(),
            regr::regr_syy_udaf(),
            regr::regr_sxy_udaf(),
        ] {
            continuation(udf, vec![x.clone(), y.clone()])?;
        }
        Ok(())
    }

    #[derive(Debug)]
    struct ExportProbe {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl Accumulator for ExportProbe {
        fn update_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
            unreachable!()
        }
        fn merge_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
            unreachable!()
        }
        fn evaluate(&mut self) -> Result<ScalarValue> {
            unreachable!()
        }
        fn size(&self) -> usize {
            std::mem::size_of::<Self>()
        }
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            assert_eq!(
                self.calls.fetch_add(1, Ordering::SeqCst),
                0,
                "state exported twice"
            );
            if self.fail {
                datafusion::common::exec_err!("injected export failure")
            } else {
                Ok(vec![ScalarValue::Int64(Some(2))])
            }
        }
    }

    #[test]
    fn snapshot_exports_once_rebuilds_and_propagates_failures() -> Result<()> {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let expr = expression(count::count_udaf(), std::slice::from_ref(&values));
        let fields = state_fields(&expr)?.unwrap();
        for (fail, malformed_schema) in [(false, false), (true, false), (false, true)] {
            let calls = Arc::new(AtomicUsize::new(0));
            let mut acc: Box<dyn Accumulator> = Box::new(ExportProbe {
                calls: calls.clone(),
                fail,
            });
            let result = snapshot(
                &expr,
                if malformed_schema { &[] } else { &fields },
                &mut acc,
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            if fail || malformed_schema {
                assert!(result.is_err());
            } else {
                assert_eq!(result?, vec![ScalarValue::Int64(Some(2))]);
                acc.update_batch(std::slice::from_ref(&values))?;
                assert_eq!(acc.evaluate()?, ScalarValue::Int64(Some(3)));
            }
        }
        Ok(())
    }
}
