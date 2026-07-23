// Parsing for arroyo's `CREATE TABLE` DDL extensions on top of an unmodified upstream sqlparser.
//
// The ArroyoSystems sqlparser fork added three streaming-DDL forms to `CREATE TABLE`:
//   * `WATERMARK FOR <col> [AS <expr>]`        (table constraint)
//   * `<col> <type> METADATA FROM '<key>'`     (column option)
//   * `PARTITIONED BY (<expr>, ...)`           (trailing clause, Iceberg transforms)
// Upstream sqlparser can't parse these, and upstream DataFusion needs an unmodified sqlparser, so
// instead of forking we parse connector/memory `CREATE TABLE` statements here using sqlparser's public
// `Parser` API and surface the result as our own `ParsedCreateTable`. Everything else (queries, INSERT,
// CREATE VIEW, CTAS) is parsed by vanilla sqlparser and handed to DataFusion's `SqlToRel` unchanged.

use sqlparser::ast::{DataType, Expr, Ident, ObjectName, SqlOption};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

/// A column in an arroyo `CREATE TABLE`, carrying only the options arroyo actually consumes.
#[derive(Debug, Clone)]
pub struct ParsedColumn {
    pub name: Ident,
    pub data_type: DataType,
    /// `NOT NULL` was specified.
    pub not_null: bool,
    /// `PRIMARY KEY` column option.
    pub primary_key: bool,
    /// Generated-column expression (`GENERATED ALWAYS AS (expr) [STORED]` or `AS (expr)`).
    pub generated: Option<Expr>,
    /// `METADATA FROM '<key>'` — the metadata key this column is populated from.
    pub metadata_key: Option<String>,
}

/// An arroyo connector/memory `CREATE TABLE` statement (i.e. `CREATE TABLE … ` with no `AS <query>`).
#[derive(Debug, Clone)]
pub struct ParsedCreateTable {
    pub name: ObjectName,
    pub temporary: bool,
    pub columns: Vec<ParsedColumn>,
    /// Columns named by a table-level `PRIMARY KEY (...)` constraint.
    pub primary_key_columns: Vec<Ident>,
    /// `WATERMARK FOR <col> [AS <expr>]`, if present.
    pub watermark: Option<(Ident, Option<Expr>)>,
    /// `WITH (...)` options, as `SqlOption::KeyValue` pairs (what `ConnectorOptions` consumes).
    pub with_options: Vec<SqlOption>,
    /// `PARTITIONED BY (...)` transform expressions.
    pub partitions: Option<Vec<Expr>>,
}

fn keyword_at(parser: &Parser, n: usize, kw: Keyword) -> bool {
    matches!(parser.peek_nth_token(n).token, Token::Word(w) if w.keyword == kw)
}

/// Consume the next token iff it is an (unquoted) word equal to `word` (case-insensitive).
/// Used for arroyo's extension keywords (`WATERMARK`/`METADATA`/`PARTITIONED`) which are not
/// reserved words in upstream sqlparser, so they tokenize as ordinary identifiers.
fn parse_word(parser: &mut Parser, word: &str) -> bool {
    match &parser.peek_token().token {
        Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word) => {
            parser.next_token();
            true
        }
        _ => false,
    }
}

/// Returns true if the statement at the parser's current position is a connector/memory `CREATE TABLE`
/// (a `CREATE TABLE` with no `AS <query>`), which we parse ourselves. `CREATE TABLE … AS …` (CTAS),
/// `CREATE VIEW`, queries, etc. return false and are left to vanilla sqlparser + DataFusion.
pub fn is_connector_table(parser: &Parser) -> bool {
    // CREATE [OR REPLACE] [GLOBAL|LOCAL] [TEMP|TEMPORARY] TABLE ...
    let mut i = 0;
    if !keyword_at(parser, i, Keyword::CREATE) {
        return false;
    }
    i += 1;
    if keyword_at(parser, i, Keyword::OR) && keyword_at(parser, i + 1, Keyword::REPLACE) {
        i += 2;
    }
    if keyword_at(parser, i, Keyword::GLOBAL) || keyword_at(parser, i, Keyword::LOCAL) {
        i += 1;
    }
    if keyword_at(parser, i, Keyword::TEMP) || keyword_at(parser, i, Keyword::TEMPORARY) {
        i += 1;
    }
    if !keyword_at(parser, i, Keyword::TABLE) {
        return false;
    }

    // It's a CREATE TABLE. Distinguish a connector/memory table from CTAS by scanning for a
    // depth-0 `AS` (=> CTAS, leave to DataFusion) vs `WITH` / end-of-statement (=> our table).
    // A generated column's `AS` lives inside the column-list parens (depth > 0), so it won't match.
    let mut depth: i32 = 0;
    let mut j = i + 1;
    loop {
        match parser.peek_nth_token(j).token {
            Token::EOF | Token::SemiColon => return true,
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(w) if depth == 0 && w.keyword == Keyword::AS => return false,
            Token::Word(w) if depth == 0 && w.keyword == Keyword::WITH => return true,
            _ => {}
        }
        j += 1;
        if j > 100_000 {
            return false;
        }
    }
}

fn parse_with_option(parser: &mut Parser) -> Result<SqlOption, ParserError> {
    let key = parser.parse_identifier()?;
    parser.expect_token(&Token::Eq)?;
    let value = parser.parse_expr()?;
    Ok(SqlOption::KeyValue { key, value })
}

fn parse_column(parser: &mut Parser) -> Result<ParsedColumn, ParserError> {
    let name = parser.parse_identifier()?;
    let data_type = parser.parse_data_type()?;

    let mut not_null = false;
    let mut primary_key = false;
    let mut generated = None;
    let mut metadata_key = None;

    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            not_null = true;
        } else if parser.parse_keyword(Keyword::NULL) {
            // explicitly nullable (the default); nothing to record
        } else if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            primary_key = true;
        } else if parse_word(parser, "METADATA") {
            parser.expect_keyword(Keyword::FROM)?;
            metadata_key = Some(parser.parse_literal_string()?);
        } else if parser.parse_keyword(Keyword::GENERATED) {
            let _ = parser.parse_keyword(Keyword::ALWAYS);
            parser.expect_keyword(Keyword::AS)?;
            parser.expect_token(&Token::LParen)?;
            generated = Some(parser.parse_expr()?);
            parser.expect_token(&Token::RParen)?;
            let _ = parser.parse_keyword(Keyword::STORED);
        } else if parser.parse_keyword(Keyword::AS) {
            // generated-column shorthand: `AS (expr)`
            parser.expect_token(&Token::LParen)?;
            generated = Some(parser.parse_expr()?);
            parser.expect_token(&Token::RParen)?;
        } else if parser.parse_keyword(Keyword::DEFAULT) {
            // arroyo doesn't use DEFAULT, but accept and discard it to stay compatible
            let _ = parser.parse_expr()?;
        } else {
            break;
        }
    }

    Ok(ParsedColumn {
        name,
        data_type,
        not_null,
        primary_key,
        generated,
        metadata_key,
    })
}

/// Parse one connector/memory `CREATE TABLE` statement. Assumes `is_connector_table` returned true.
pub fn parse_create_table(parser: &mut Parser) -> Result<ParsedCreateTable, ParserError> {
    parser.expect_keyword(Keyword::CREATE)?;
    let _ = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let _ = parser.parse_one_of_keywords(&[Keyword::GLOBAL, Keyword::LOCAL]);
    let temporary = parser
        .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
        .is_some();
    parser.expect_keyword(Keyword::TABLE)?;
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parser.parse_object_name(false)?;

    let mut columns = Vec::new();
    let mut primary_key_columns = Vec::new();
    let mut watermark = None;

    if parser.consume_token(&Token::LParen) && !parser.consume_token(&Token::RParen) {
        loop {
            if parse_word(parser, "WATERMARK") {
                if watermark.is_some() {
                    return Err(ParserError::ParserError(
                        "only one WATERMARK FOR constraint is allowed per table".to_string(),
                    ));
                }
                parser.expect_keyword(Keyword::FOR)?;
                let column_name = parser.parse_identifier()?;
                let watermark_expr = if parser.parse_keyword(Keyword::AS) {
                    Some(parser.parse_expr()?)
                } else {
                    None
                };
                watermark = Some((column_name, watermark_expr));
            } else if parser.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
                parser.expect_token(&Token::LParen)?;
                primary_key_columns.extend(parser.parse_comma_separated(Parser::parse_identifier)?);
                parser.expect_token(&Token::RParen)?;
            } else {
                columns.push(parse_column(parser)?);
            }

            if parser.consume_token(&Token::Comma) {
                if parser.consume_token(&Token::RParen) {
                    break; // trailing comma
                }
                continue;
            }
            parser.expect_token(&Token::RParen)?;
            break;
        }
    }

    let with_options = if parser.parse_keyword(Keyword::WITH) {
        parser.expect_token(&Token::LParen)?;
        let opts = parser.parse_comma_separated(parse_with_option)?;
        parser.expect_token(&Token::RParen)?;
        opts
    } else {
        Vec::new()
    };

    let partitions = if parse_word(parser, "PARTITIONED") {
        parser.expect_keyword(Keyword::BY)?;
        parser.expect_token(&Token::LParen)?;
        let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
        parser.expect_token(&Token::RParen)?;
        Some(exprs)
    } else {
        None
    };

    Ok(ParsedCreateTable {
        name,
        temporary,
        columns,
        primary_key_columns,
        watermark,
        with_options,
        partitions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::ArroyoDialect;

    /// Parse a statement we expect `is_connector_table` to claim, asserting that routing first.
    fn parse(sql: &str) -> ParsedCreateTable {
        let dialect = ArroyoDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        assert!(
            is_connector_table(&parser),
            "expected a connector/memory CREATE TABLE for: {sql}"
        );
        parse_create_table(&mut parser).unwrap()
    }

    fn try_parse(sql: &str) -> Result<ParsedCreateTable, ParserError> {
        let dialect = ArroyoDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        parse_create_table(&mut parser)
    }

    /// Whether `parse_program` would dispatch this statement to arroyo's DDL parser.
    fn routes_to_ddl(sql: &str) -> bool {
        let dialect = ArroyoDialect {};
        let parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        is_connector_table(&parser)
    }

    #[test]
    fn routing_distinguishes_connector_tables_from_queries() {
        // Connector/memory CREATE TABLE (no `AS <query>`) → parsed by arroyo.
        assert!(routes_to_ddl("CREATE TABLE foo (x BIGINT)"));
        assert!(routes_to_ddl(
            "CREATE TABLE foo (x BIGINT) WITH ('connector' = 'kafka')"
        ));
        assert!(routes_to_ddl(
            "CREATE TABLE impulse WITH (connector = 'impulse')"
        ));
        assert!(routes_to_ddl("CREATE OR REPLACE TABLE foo (x BIGINT)"));
        assert!(routes_to_ddl("CREATE TEMPORARY TABLE foo (x BIGINT)"));
        assert!(routes_to_ddl("CREATE TABLE IF NOT EXISTS foo (x BIGINT)"));

        // Everything else → left to vanilla sqlparser + DataFusion.
        assert!(!routes_to_ddl("CREATE TABLE foo AS SELECT 1"));
        assert!(!routes_to_ddl(
            "CREATE TABLE foo AS WITH t AS (SELECT 1) SELECT * FROM t"
        ));
        assert!(!routes_to_ddl("CREATE VIEW v AS SELECT 1"));
        assert!(!routes_to_ddl("SELECT 1"));
        assert!(!routes_to_ddl("INSERT INTO foo VALUES (1)"));
    }

    #[test]
    fn generated_column_as_is_not_mistaken_for_ctas() {
        // The `AS (expr)` of a generated column sits inside the column-list parens (depth > 0),
        // so the depth-0 `AS` scan in `is_connector_table` must not treat it as CTAS.
        assert!(routes_to_ddl(
            "CREATE TABLE foo (x BIGINT, y BIGINT GENERATED ALWAYS AS (x + 1) STORED) \
             WITH ('connector' = 'kafka')"
        ));
        assert!(routes_to_ddl(
            "CREATE TABLE foo (x BIGINT, y BIGINT AS (x + 1))"
        ));
    }

    #[test]
    fn parses_columns_with_types_and_nullability() {
        let t = parse("CREATE TABLE foo (a BIGINT, b TEXT NOT NULL, c INT NULL)");
        assert_eq!(t.name.to_string(), "foo");
        assert_eq!(t.columns.len(), 3);

        assert_eq!(t.columns[0].name.value, "a");
        assert_eq!(t.columns[0].data_type.to_string(), "BIGINT");
        assert!(!t.columns[0].not_null, "columns are nullable by default");

        assert_eq!(t.columns[1].name.value, "b");
        assert_eq!(t.columns[1].data_type.to_string(), "TEXT");
        assert!(t.columns[1].not_null);

        assert_eq!(t.columns[2].name.value, "c");
        assert!(!t.columns[2].not_null, "explicit NULL is still nullable");
    }

    #[test]
    fn parses_primary_keys_inline_and_table_level() {
        let inline = parse("CREATE TABLE foo (id BIGINT NOT NULL PRIMARY KEY, x INT)");
        assert!(inline.columns[0].primary_key);
        assert!(inline.columns[0].not_null);
        assert!(!inline.columns[1].primary_key);
        assert!(inline.primary_key_columns.is_empty());

        let table_level = parse("CREATE TABLE foo (a INT, b INT, PRIMARY KEY (a, b))");
        assert!(!table_level.columns[0].primary_key);
        let pk: Vec<_> = table_level
            .primary_key_columns
            .iter()
            .map(|i| i.value.clone())
            .collect();
        assert_eq!(pk, vec!["a", "b"]);
    }

    #[test]
    fn parses_watermark_with_and_without_expression() {
        let with_expr = parse(
            "CREATE TABLE foo (event_time TIMESTAMP, \
             WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND)",
        );
        let (col, expr) = with_expr.watermark.expect("watermark present");
        assert_eq!(col.value, "event_time");
        let rendered = expr.expect("watermark expr present").to_string();
        assert!(
            rendered.contains("event_time") && rendered.contains("INTERVAL"),
            "unexpected watermark expr: {rendered}"
        );

        let no_expr = parse("CREATE TABLE foo (event_time TIMESTAMP, WATERMARK FOR event_time)");
        let (col, expr) = no_expr.watermark.expect("watermark present");
        assert_eq!(col.value, "event_time");
        assert!(expr.is_none());
    }

    #[test]
    fn rejects_multiple_watermarks() {
        let err = try_parse(
            "CREATE TABLE foo (a TIMESTAMP, b TIMESTAMP, WATERMARK FOR a, WATERMARK FOR b)",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("only one WATERMARK"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_metadata_from() {
        let t = parse("CREATE TABLE foo (name TEXT, my_topic TEXT METADATA FROM 'topic')");
        assert_eq!(t.columns[0].metadata_key, None);
        assert_eq!(t.columns[1].metadata_key.as_deref(), Some("topic"));
    }

    #[test]
    fn parses_generated_columns() {
        // `GENERATED ALWAYS AS (expr) [STORED]`
        let stored = parse(
            "CREATE TABLE foo (date_string TEXT, \
             ts TIMESTAMP GENERATED ALWAYS AS (CAST(date_string AS TIMESTAMP)) STORED)",
        );
        assert!(stored.columns[0].generated.is_none());
        assert!(stored.columns[1].generated.is_some());

        // shorthand `AS (expr)`
        let shorthand = parse("CREATE TABLE foo (x BIGINT, y BIGINT AS (x + 1))");
        assert_eq!(
            shorthand.columns[1].generated.as_ref().unwrap().to_string(),
            "x + 1"
        );
    }

    #[test]
    fn parses_with_options_including_quoted_and_dotted_keys() {
        // Connectors like iceberg use single-quoted, dotted option keys (e.g. 'catalog.type').
        // These can't be bare identifiers, so the WITH-option parser must accept quoted keys and
        // expose them with the quotes stripped (ConnectorOptions keys on `Ident::value`).
        let t = parse(
            "CREATE TABLE foo (x BIGINT) WITH (\
             connector = 'kafka', \
             event_rate = 100, \
             'catalog.type' = 'rest', \
             'shuffle_by_partition.enabled' = true)",
        );
        let opts: Vec<(String, String)> = t
            .with_options
            .iter()
            .map(|o| match o {
                SqlOption::KeyValue { key, value } => (key.value.clone(), value.to_string()),
                other => panic!("unexpected option form: {other:?}"),
            })
            .collect();
        assert_eq!(
            opts,
            vec![
                ("connector".to_string(), "'kafka'".to_string()),
                ("event_rate".to_string(), "100".to_string()),
                ("catalog.type".to_string(), "'rest'".to_string()),
                (
                    "shuffle_by_partition.enabled".to_string(),
                    "true".to_string()
                ),
            ]
        );
    }

    #[test]
    fn parses_partitioned_by_transforms() {
        let t = parse(
            "CREATE TABLE foo (id INT, b TEXT, ts TIMESTAMP) WITH (connector = 'iceberg') \
             PARTITIONED BY (bucket(id, 4), truncate(b, 10), identity(ts))",
        );
        let parts = t.partitions.expect("partitions present");
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].to_string(), "bucket(id, 4)");
        assert_eq!(parts[2].to_string(), "identity(ts)");
    }

    #[test]
    fn parses_temporary_and_if_not_exists() {
        let temp = parse("CREATE TEMPORARY TABLE foo (x BIGINT)");
        assert!(temp.temporary);
        assert_eq!(temp.name.to_string(), "foo");

        let if_not_exists = parse("CREATE TABLE IF NOT EXISTS bar (x BIGINT)");
        assert!(!if_not_exists.temporary);
        assert_eq!(if_not_exists.name.to_string(), "bar");
    }

    #[test]
    fn parses_trailing_comma_in_column_list() {
        let t = parse("CREATE TABLE foo (a INT, b INT,)");
        assert_eq!(t.columns.len(), 2);
    }

    #[test]
    fn parses_real_world_watermark_table() {
        // Mirrors test/queries/watermarks.sql, including `timestamp` used as a column name
        // (a non-reserved keyword) and a lowercase `watermark ... as` constraint.
        let t = parse(
            "CREATE TABLE orders (\
               customer_id INT, \
               order_id INT, \
               date_string TEXT NOT NULL, \
               timestamp TIMESTAMP GENERATED ALWAYS AS (CAST(date_string AS TIMESTAMP)), \
               watermark FOR timestamp as CAST(date_string AS TIMESTAMP) - INTERVAL '5 seconds'\
             ) WITH (\
               connector = 'kafka', \
               format = 'json', \
               type = 'source', \
               topic = 'order_topic'\
             )",
        );
        assert_eq!(t.columns.len(), 4);
        assert_eq!(t.columns[3].name.value, "timestamp");
        assert!(t.columns[3].generated.is_some());
        let (wm_col, wm_expr) = t.watermark.expect("watermark present");
        assert_eq!(wm_col.value, "timestamp");
        assert!(wm_expr.is_some());
        assert_eq!(t.with_options.len(), 4);
    }
}
