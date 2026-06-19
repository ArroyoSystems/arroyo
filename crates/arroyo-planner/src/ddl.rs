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
