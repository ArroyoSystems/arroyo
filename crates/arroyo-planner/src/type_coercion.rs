use arrow_schema::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{DFSchema, DataFusionError, Diagnostic, Result};
use datafusion::logical_expr::expr::{InSubquery, SetComparison};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator, TypeSignature};
use datafusion::optimizer::Analyzer;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::{TypeCoercion, TypeCoercionRewriter};
use datafusion::optimizer::utils::NamePreserver;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn analyze_plan(plan: LogicalPlan, analyzer: &Analyzer) -> Result<LogicalPlan> {
    let mut analyzer = analyzer.clone();
    let coercion_index = analyzer
        .rules
        .iter()
        .position(|rule| rule.name() == TypeCoercion::new().name())
        .unwrap_or(0);
    analyzer.rules.insert(
        coercion_index,
        Arc::new(RejectImplicitStringNumericComparisons),
    );
    analyzer.execute_and_check(plan, &ConfigOptions::default(), |_, _| {})
}

#[derive(Debug)]
struct RejectImplicitStringNumericComparisons;

impl AnalyzerRule for RejectImplicitStringNumericComparisons {
    fn name(&self) -> &str {
        "reject_implicit_string_numeric_comparisons"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Analyze a scratch plan bottom-up so parent expressions see the actual types
        // of CASE results, function results, and subquery outputs. Keep the original
        // plan for DataFusion's normal analyzer, including its configured rewrites.
        plan.clone().transform_up_with_subqueries(validate_plan)?;
        Ok(plan)
    }
}

fn validate_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = &plan {
        schema.merge(&DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?);
    }

    let mut validator = ComparisonValidator { schema: &schema };
    let name_preserver = NamePreserver::new(&plan);
    let plan = plan
        .map_expressions(|expr| {
            let name = name_preserver.save(&expr);
            expr.rewrite(&mut validator)
                .map(|transformed| transformed.update_data(|expr| name.restore(expr)))
        })?
        .data;

    // Equijoin keys are stored as pairs, not BinaryExprs.
    if let LogicalPlan::Join(join) = &plan {
        for (left, right) in &join.on {
            validator.check([left, right])?;
        }
    }

    let plan = TypeCoercionRewriter::new(&schema).coerce_plan(plan)?;
    Ok(Transformed::yes(plan.recompute_schema()?))
}

struct ComparisonValidator<'a> {
    schema: &'a DFSchema,
}

impl ComparisonValidator<'_> {
    fn check<'a>(&self, args: impl IntoIterator<Item = &'a Expr>) -> Result<()> {
        let mut args = args.into_iter();
        let Some(mut previous) = args.next() else {
            return Ok(());
        };
        let mut common_type = previous.get_type(self.schema)?;
        for arg in args {
            let data_type = arg.get_type(self.schema)?;
            if has_string_numeric_comparison(&common_type, &data_type) {
                let message = format!(
                    "Cannot implicitly compare {previous} with {arg}: operand types {common_type} and {data_type} mix strings and numbers. \
                     Use matching string or numeric operands, or an explicit CAST or TRY_CAST. \
                     CAST can fail on invalid input; TRY_CAST returns NULL."
                );
                let span = previous
                    .spans()
                    .and_then(|spans| spans.first())
                    .or_else(|| arg.spans().and_then(|spans| spans.first()));
                return Err(DataFusionError::Plan(message.clone())
                    .with_diagnostic(Diagnostic::new_error(message, span)));
            }

            // Accumulate the common type so NULLs and partially typed nested operands
            // cannot hide a string/numeric conflict in multi-argument comparisons.
            let Some(coerced_type) = comparison_coercion(&common_type, &data_type) else {
                // DataFusion will reject the incompatible operand types itself.
                return Ok(());
            };
            if coerced_type == data_type {
                previous = arg;
            }
            common_type = coerced_type;
        }
        Ok(())
    }
}

fn has_string_numeric_comparison(left: &DataType, right: &DataType) -> bool {
    use DataType::*;

    match (left, right) {
        (Dictionary(_, value), other) | (other, Dictionary(_, value)) => {
            has_string_numeric_comparison(value, other)
        }
        (RunEndEncoded(_, value), other) | (other, RunEndEncoded(_, value)) => {
            has_string_numeric_comparison(value.data_type(), other)
        }
        (
            List(left) | LargeList(left) | FixedSizeList(left, _),
            List(right) | LargeList(right) | FixedSizeList(right, _),
        )
        | (Map(left, _), Map(right, _)) => {
            has_string_numeric_comparison(left.data_type(), right.data_type())
        }
        (Struct(left), Struct(right)) if left.len() == right.len() => {
            let right_by_name: HashMap<_, _> = right
                .iter()
                .map(|field| (field.name().as_str(), field))
                .collect();
            // Like DataFusion, match by name when the name sets agree, otherwise
            // positionally. Do not compare unrelated fields in a mixed-type struct.
            let by_name = left
                .iter()
                .all(|field| right_by_name.contains_key(field.name().as_str()));
            left.iter().zip(right).any(|(left, right)| {
                let right = if by_name {
                    right_by_name
                        .get(left.name().as_str())
                        .copied()
                        .unwrap_or(right)
                } else {
                    right
                };
                has_string_numeric_comparison(left.data_type(), right.data_type())
            })
        }
        _ => (left.is_string() && right.is_numeric()) || (left.is_numeric() && right.is_string()),
    }
}

impl TreeNodeRewriter for ComparisonValidator<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match &expr {
            Expr::BinaryExpr(binary)
                if matches!(
                    binary.op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::IsDistinctFrom
                        | Operator::IsNotDistinctFrom
                ) =>
            {
                self.check([binary.left.as_ref(), binary.right.as_ref()])?;
            }
            Expr::Between(between) => {
                self.check([
                    between.expr.as_ref(),
                    between.low.as_ref(),
                    between.high.as_ref(),
                ])?;
            }
            Expr::InList(list) => {
                self.check(std::iter::once(list.expr.as_ref()).chain(list.list.iter()))?;
            }
            Expr::Case(case) => {
                if let Some(operand) = &case.expr {
                    self.check(
                        std::iter::once(operand.as_ref())
                            .chain(case.when_then_expr.iter().map(|(when, _)| when.as_ref())),
                    )?;
                }
            }
            Expr::ScalarFunction(function)
                if matches!(
                    function.func.signature().type_signature,
                    TypeSignature::Comparable(_)
                ) || matches!(function.func.name(), "greatest" | "least") =>
            {
                self.check(&function.args)?;
            }
            Expr::InSubquery(InSubquery { expr, subquery, .. })
            | Expr::SetComparison(SetComparison { expr, subquery, .. }) => {
                self.check([expr.as_ref(), &Expr::ScalarSubquery(subquery.clone())])?;
            }
            _ => {}
        }

        TypeCoercionRewriter::new(self.schema).f_up(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::{analyze_plan, has_string_numeric_comparison};
    use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::datasource::MemTable;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionContext;
    use rstest::rstest;
    use std::sync::Arc;
    use test_log::test;

    async fn analyze(sql: &str, string_type: DataType) -> datafusion::common::Result<()> {
        let context = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", string_type, true),
            Field::new("number", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        context.register_table("test", Arc::new(MemTable::try_new(schema, vec![vec![]])?))?;
        let plan = context.state().create_logical_plan(sql).await?;
        let analyzer = Analyzer::new();
        let validated = analyze_plan(plan.clone(), &analyzer)?;
        let baseline = analyzer.execute_and_check(plan, &ConfigOptions::default(), |_, _| {})?;
        assert_eq!(validated, baseline);
        Ok(())
    }

    #[rstest]
    #[case("SELECT * FROM test WHERE user_id != 123")]
    #[case("SELECT * FROM test WHERE 123 != user_id")]
    #[case("SELECT * FROM test WHERE number = '123'")]
    #[case("SELECT * FROM test WHERE user_id < 1.5")]
    #[case("SELECT * FROM test WHERE user_id IS DISTINCT FROM number")]
    #[case("SELECT * FROM test WHERE user_id IN (1, 2)")]
    #[case("SELECT * FROM test WHERE user_id NOT IN ('123', 2)")]
    #[case("SELECT * FROM test WHERE NULL IN ('123', 2)")]
    #[case("SELECT * FROM test WHERE user_id BETWEEN 1 AND 2")]
    #[case("SELECT * FROM test WHERE NULL BETWEEN '123' AND 2")]
    #[case("SELECT CASE user_id WHEN 123 THEN 1 ELSE 0 END FROM test")]
    #[case("SELECT CASE number WHEN '123' THEN 1 ELSE 0 END FROM test")]
    #[case("SELECT GREATEST(user_id, number) FROM test")]
    #[case("SELECT LEAST(NULL, user_id, number) FROM test")]
    #[case("SELECT NULLIF(user_id, 123) FROM test")]
    #[case("SELECT NULLIF(number, user_id) FROM test")]
    #[case("SELECT * FROM test WHERE NULLIF(user_id, 123) IS NOT NULL")]
    #[case("SELECT * FROM test WHERE [user_id] = [123]")]
    #[case("SELECT * FROM test WHERE [[user_id]] = [[123]]")]
    #[case("SELECT * FROM test WHERE NULL IN ([user_id], [123])")]
    #[case("SELECT NULLIF([user_id], [123]) FROM test")]
    #[case("SELECT * FROM test WHERE named_struct('id', user_id) = named_struct('id', number)")]
    #[case(
        "SELECT * FROM test WHERE named_struct('left', user_id) = named_struct('right', number)"
    )]
    #[case(
        "SELECT * FROM test WHERE named_struct('id', user_id, 'number', number) = named_struct('number', number, 'id', number)"
    )]
    #[case(
        "SELECT * FROM test WHERE NULL IN (named_struct('id', user_id, 'other', NULL), named_struct('id', NULL, 'other', user_id), named_struct('id', NULL, 'other', number))"
    )]
    #[case("SELECT a.user_id FROM test a JOIN test b ON a.user_id = b.number")]
    #[case("SELECT a.user_id FROM test a JOIN test b ON a.user_id < b.number")]
    #[case(
        "SELECT a.user_id FROM test a JOIN (SELECT number AS user_id FROM test) b USING (user_id)"
    )]
    #[case("SELECT a.user_id FROM test a JOIN test b USING (user_id) WHERE b.user_id = 1")]
    #[case("SELECT * FROM test WHERE user_id IN (SELECT number FROM test)")]
    #[case("SELECT * FROM test WHERE number = (SELECT user_id FROM test LIMIT 1)")]
    #[case("SELECT * FROM test WHERE number = ANY (SELECT user_id FROM test)")]
    #[case("SELECT * FROM test WHERE EXISTS (SELECT 1 FROM test b WHERE b.user_id = test.number)")]
    #[case("WITH cte AS (SELECT user_id AS id FROM test) SELECT * FROM cte WHERE id = 123")]
    #[case(
        "WITH cte AS (SELECT CASE WHEN flag THEN 123 ELSE user_id END AS id FROM test) SELECT * FROM cte WHERE id = 123"
    )]
    #[case("SELECT * FROM test WHERE CAST(user_id = 123 AS BOOLEAN)")]
    #[case("SELECT * FROM test WHERE (CASE WHEN flag THEN 123 ELSE user_id END) = 123")]
    #[tokio::test]
    async fn rejects_implicit_comparisons(#[case] sql: &str) {
        for string_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let error = analyze(sql, string_type).await.unwrap_err();
            assert!(
                error.to_string().contains("Cannot implicitly compare"),
                "{sql}: {error}"
            );
        }
    }

    #[rstest]
    #[case("SELECT * FROM test WHERE user_id != '123'")]
    #[case("SELECT * FROM test WHERE number = 123")]
    #[case("SELECT * FROM test WHERE number < 1.5")]
    #[case("SELECT * FROM test WHERE user_id IS NULL")]
    #[case("SELECT * FROM test WHERE user_id = NULL")]
    #[case("SELECT * FROM test WHERE CAST(user_id AS BIGINT) != 123")]
    #[case("SELECT * FROM test WHERE TRY_CAST(user_id AS BIGINT) != 123")]
    #[case("SELECT * FROM test WHERE user_id != CAST(number AS TEXT)")]
    #[case("SELECT * FROM test WHERE TRY_CAST(user_id AS BIGINT) IN (1, 2)")]
    #[case("SELECT * FROM test WHERE CAST(user_id AS BIGINT) BETWEEN 1 AND 2")]
    #[case("SELECT CASE CAST(user_id AS BIGINT) WHEN 123 THEN 1 ELSE 0 END FROM test")]
    #[case("SELECT GREATEST(TRY_CAST(user_id AS BIGINT), number) FROM test")]
    #[case("SELECT LEAST(user_id, '123') FROM test")]
    #[case("SELECT NULLIF(user_id, '123') FROM test")]
    #[case("SELECT NULLIF(CAST(user_id AS BIGINT), 123) FROM test")]
    #[case("SELECT NULLIF(TRY_CAST(user_id AS BIGINT), 123) FROM test")]
    #[case("SELECT * FROM test WHERE [user_id] = ['123']")]
    #[case("SELECT * FROM test WHERE [CAST(user_id AS BIGINT)] = [123]")]
    #[case("SELECT * FROM test WHERE [TRY_CAST(user_id AS BIGINT)] = [123]")]
    #[case("SELECT * FROM test WHERE CAST([user_id] AS BIGINT[]) = [123]")]
    #[case("SELECT * FROM test WHERE TRY_CAST([user_id] AS BIGINT[]) = [123]")]
    #[case("SELECT * FROM test WHERE [number] = [1.5]")]
    #[case("SELECT NULLIF([user_id], ['123']) FROM test")]
    #[case("SELECT * FROM test WHERE NULL IN ([NULL], [number], [1.5])")]
    #[case(
        "SELECT * FROM test WHERE named_struct('id', user_id, 'number', number) = named_struct('number', number, 'id', user_id)"
    )]
    #[case(
        "SELECT * FROM test WHERE named_struct('id', CAST(user_id AS BIGINT)) = named_struct('id', number)"
    )]
    #[case(
        "SELECT * FROM test WHERE named_struct('id', TRY_CAST(user_id AS BIGINT)) = named_struct('id', number)"
    )]
    #[case(
        "SELECT * FROM test WHERE NULL IN (named_struct('id', user_id, 'other', NULL), named_struct('id', NULL, 'other', user_id), named_struct('id', user_id, 'other', user_id))"
    )]
    #[case("SELECT a.user_id FROM test a JOIN test b ON TRY_CAST(a.user_id AS BIGINT) = b.number")]
    #[case("SELECT * FROM test WHERE TRY_CAST(user_id AS BIGINT) IN (SELECT number FROM test)")]
    #[case("SELECT * FROM test WHERE length(user_id) > 123")]
    #[case(
        "SELECT * FROM test WHERE EXISTS (SELECT 1 FROM test b WHERE TRY_CAST(b.user_id AS BIGINT) = test.number)"
    )]
    #[case("SELECT * FROM test WHERE (CASE WHEN flag THEN 123 ELSE user_id END) = '123'")]
    #[case("SELECT (CASE WHEN flag THEN 123 ELSE user_id END) FROM test")]
    #[case(
        "WITH cte AS (SELECT CASE WHEN flag THEN 123 ELSE user_id END AS id FROM test) SELECT * FROM cte WHERE id = '123'"
    )]
    #[tokio::test]
    async fn allows_matching_or_explicit_comparisons(#[case] sql: &str) {
        for string_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            analyze(sql, string_type).await.unwrap();
        }
    }

    #[test]
    fn checks_nested_type_wrappers() {
        let field = |data_type| Arc::new(Field::new("item", data_type, true));
        let map = |value_type| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", value_type, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        for (left, right) in [
            (
                DataType::List(field(DataType::Utf8)),
                DataType::LargeList(field(DataType::Int64)),
            ),
            (
                DataType::FixedSizeList(field(DataType::Utf8View), 1),
                DataType::List(field(DataType::Int64)),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                DataType::Int64,
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    field(DataType::LargeUtf8),
                ),
                DataType::Int64,
            ),
            (map(DataType::Utf8), map(DataType::Int64)),
        ] {
            assert!(
                has_string_numeric_comparison(&left, &right),
                "{left} vs {right}"
            );
            assert!(
                has_string_numeric_comparison(&right, &left),
                "{right} vs {left}"
            );
            assert!(!has_string_numeric_comparison(&left, &left));
            assert!(!has_string_numeric_comparison(&right, &right));
        }
    }

    async fn compile_inventory_query(
        predicate: &str,
    ) -> Result<crate::CompiledSql, crate::PlannerError> {
        let sql = format!(
            "CREATE TABLE inventory_stream (user_id TEXT NOT NULL) WITH (
                connector = 'kafka', bootstrap_servers = 'localhost:9092',
                topic = 'inventory', format = 'json', type = 'source'
            );
            CREATE TABLE inventory_sink (user_id TEXT NOT NULL) WITH (
                connector = 'kafka', bootstrap_servers = 'localhost:9092',
                topic = 'inventory-output', format = 'json', type = 'sink'
            );
            INSERT INTO inventory_sink SELECT * FROM inventory_stream WHERE {predicate};"
        );
        parse_and_get_program(&sql, ArroyoSchemaProvider::new(), SqlConfig::default()).await
    }

    #[test(tokio::test)]
    async fn rejects_implicit_string_numeric_pipeline() {
        let error = compile_inventory_query("user_id != 123").await.unwrap_err();
        let diagnostic = &error.diagnostics[0];
        assert!(diagnostic.message.contains("Cannot implicitly compare"));
        assert!(diagnostic.message.contains("user_id"));
        assert!(diagnostic.message.contains("Utf8"));
        assert!(diagnostic.message.contains("Int64"));
        assert!(diagnostic.message.contains("TRY_CAST"));
        assert!(diagnostic.span.is_some());
    }

    #[test(tokio::test)]
    async fn rejects_implicit_function_and_nested_pipeline() {
        for predicate in [
            "NULLIF(user_id, 123) IS NOT NULL",
            "[user_id] = [123]",
            "named_struct('id', user_id) = named_struct('id', 123)",
        ] {
            let error = compile_inventory_query(predicate).await.unwrap_err();
            let diagnostic = &error.diagnostics[0];
            assert!(
                diagnostic.message.contains("Cannot implicitly compare"),
                "{predicate}: {error:?}"
            );
            assert!(diagnostic.message.contains("user_id"));
            assert!(diagnostic.message.contains("Utf8"));
            assert!(diagnostic.message.contains("Int64"));
        }
    }

    #[test(tokio::test)]
    async fn allows_explicit_string_numeric_pipeline() {
        for predicate in [
            "user_id != '123'",
            "CAST(user_id AS BIGINT) != 123",
            "TRY_CAST(user_id AS BIGINT) != 123",
            "NULLIF(user_id, '123') IS NOT NULL",
            "NULLIF(CAST(user_id AS BIGINT), 123) IS NOT NULL",
            "NULLIF(TRY_CAST(user_id AS BIGINT), 123) IS NOT NULL",
            "[user_id] = ['123']",
            "[CAST(user_id AS BIGINT)] = [123]",
            "[TRY_CAST(user_id AS BIGINT)] = [123]",
            "CAST([user_id] AS BIGINT[]) = [123]",
            "TRY_CAST([user_id] AS BIGINT[]) = [123]",
            "named_struct('id', CAST(user_id AS BIGINT)) = named_struct('id', 123)",
            "named_struct('id', TRY_CAST(user_id AS BIGINT)) = named_struct('id', 123)",
        ] {
            compile_inventory_query(predicate).await.unwrap();
        }
    }
}
