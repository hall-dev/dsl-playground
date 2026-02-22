use crate::ast::{CallArg, Expr, Program, RecordField, Span, Stmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}..{}", self.message, self.span.start, self.span.end)
    }
}

impl std::error::Error for ParseError {}

pub fn parse_program(input: &str) -> Result<Program, ParseError> {
    let mut p = Parser { src: input, pos: 0 };
    p.parse_program()
}

struct Parser<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn parse_program(&mut self) -> Result<Program, ParseError> {
        let start = self.pos;
        let mut statements = Vec::new();
        self.skip_ws();
        while !self.eof() {
            statements.push(self.parse_stmt()?);
            self.skip_ws();
        }
        Ok(Program {
            statements,
            span: Span::new(start, self.pos),
        })
    }

    fn parse_stmt(&mut self) -> Result<Stmt, ParseError> {
        self.skip_ws();
        let start = self.pos;

        let checkpoint = self.pos;
        if let Some(name) = self.parse_ident() {
            self.skip_ws();
            if self.consume(":=") {
                self.skip_ws();
                let expr = self.parse_expr()?;
                self.skip_ws();
                self.expect(";")?;
                return Ok(Stmt::Binding {
                    name,
                    expr,
                    span: Span::new(start, self.pos),
                });
            }
        }
        self.pos = checkpoint;

        let expr = self.parse_expr()?;
        self.skip_ws();
        self.expect(";")?;
        Ok(Stmt::Pipeline {
            expr,
            span: Span::new(start, self.pos),
        })
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_pipeline()
    }

    fn parse_pipeline(&mut self) -> Result<Expr, ParseError> {
        let start = self.pos;
        let input = self.parse_compose()?;
        self.skip_ws();
        if !self.consume("|>") {
            return Ok(input);
        }
        let mut stages = Vec::new();
        loop {
            self.skip_ws();
            stages.push(self.parse_compose()?);
            self.skip_ws();
            if !self.consume("|>") {
                break;
            }
        }
        Ok(Expr::Pipeline {
            input: Box::new(input),
            stages,
            span: Span::new(start, self.pos),
        })
    }

    fn parse_compose(&mut self) -> Result<Expr, ParseError> {
        let start = self.pos;
        let mut left = self.parse_unary()?;
        loop {
            self.skip_ws();
            if !self.consume(">>") {
                break;
            }
            self.skip_ws();
            let right = self.parse_unary()?;
            left = Expr::Compose {
                left: Box::new(left),
                right: Box::new(right),
                span: Span::new(start, self.pos),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        self.skip_ws();
        if self.consume("~") {
            let start = self.pos - 1;
            self.skip_ws();
            let expr = self.parse_unary()?;
            return Ok(Expr::Inverse {
                expr: Box::new(expr),
                span: Span::new(start, self.pos),
            });
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_primary()?;
        loop {
            self.skip_ws();
            if self.consume(".") {
                let field_start = self.pos;
                let field = self.parse_ident().ok_or_else(|| ParseError {
                    message: "expected field name after '.'".to_string(),
                    span: Span::new(field_start, field_start),
                })?;
                let span = Span::new(expr.span().start, self.pos);
                expr = Expr::FieldAccess {
                    expr: Box::new(expr),
                    field,
                    span,
                };
                continue;
            }
            if self.consume("(") {
                let call_start = expr.span().start;
                let args = self.parse_call_args()?;
                self.expect(")")?;
                expr = Expr::Call {
                    callee: Box::new(expr),
                    args,
                    span: Span::new(call_start, self.pos),
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_call_args(&mut self) -> Result<Vec<CallArg>, ParseError> {
        let mut args = Vec::new();
        self.skip_ws();
        if self.peek() == Some(')') {
            return Ok(args);
        }

        loop {
            self.skip_ws();
            let arg_start = self.pos;
            if let Some(name) = self.try_parse_named_arg_name() {
                self.skip_ws();
                self.expect("=")?;
                self.skip_ws();
                let value = self.parse_subexpr_until(&[',', ')']);
                let span = Span::new(arg_start, self.pos);
                args.push(CallArg::Named { name, value, span });
            } else {
                let value = self.parse_subexpr_until(&[',', ')']);
                args.push(CallArg::Positional(value));
            }
            self.skip_ws();
            if self.consume(",") {
                continue;
            }
            break;
        }
        Ok(args)
    }

    fn try_parse_named_arg_name(&mut self) -> Option<String> {
        let checkpoint = self.pos;
        self.parse_ident()?;
        self.skip_ws();
        if self.peek() == Some('=') {
            self.pos = checkpoint;
            self.parse_ident()
        } else {
            self.pos = checkpoint;
            None
        }
    }

    fn parse_subexpr_until(&mut self, delimiters: &[char]) -> Expr {
        let start = self.pos;
        let mut depth_paren = 0usize;
        let mut depth_brack = 0usize;
        let mut depth_brace = 0usize;
        let mut in_string = false;
        let mut escaped = false;

        while let Some(c) = self.peek() {
            if in_string {
                self.pos += c.len_utf8();
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }

            match c {
                '"' => {
                    in_string = true;
                    self.pos += 1;
                }
                '(' => {
                    depth_paren += 1;
                    self.pos += 1;
                }
                ')' => {
                    if depth_paren == 0 && depth_brack == 0 && depth_brace == 0 && delimiters.contains(&')') {
                        break;
                    }
                    depth_paren = depth_paren.saturating_sub(1);
                    self.pos += 1;
                }
                '[' => {
                    depth_brack += 1;
                    self.pos += 1;
                }
                ']' => {
                    depth_brack = depth_brack.saturating_sub(1);
                    self.pos += 1;
                }
                '{' => {
                    depth_brace += 1;
                    self.pos += 1;
                }
                '}' => {
                    depth_brace = depth_brace.saturating_sub(1);
                    self.pos += 1;
                }
                _ if depth_paren == 0
                    && depth_brack == 0
                    && depth_brace == 0
                    && delimiters.contains(&c) =>
                {
                    break;
                }
                _ => self.pos += c.len_utf8(),
            }
        }

        let end = self.pos;
        let raw = self.src[start..end].trim();
        let trimmed_start = start + self.src[start..end].find(raw).unwrap_or(0);
        let span = Span::new(trimmed_start, trimmed_start + raw.len());
        if raw.is_empty() {
            return Expr::Raw {
                text: String::new(),
                span,
            };
        }

        let mut nested = Parser { src: raw, pos: 0 };
        if let Ok(expr) = nested.parse_expr() {
            nested.skip_ws();
            if nested.eof() {
                return rebase_expr(expr, span.start);
            }
        }

        Expr::Raw {
            text: raw.to_string(),
            span,
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        self.skip_ws();
        let start = self.pos;

        if self.consume("(") {
            let expr = self.parse_expr()?;
            self.skip_ws();
            self.expect(")")?;
            return Ok(expr);
        }

        if self.consume("[") {
            let mut items = Vec::new();
            self.skip_ws();
            if !self.consume("]") {
                loop {
                    self.skip_ws();
                    items.push(self.parse_expr()?);
                    self.skip_ws();
                    if self.consume(",") {
                        continue;
                    }
                    self.expect("]")?;
                    break;
                }
            }
            return Ok(Expr::Array {
                items,
                span: Span::new(start, self.pos),
            });
        }

        if self.consume("{") {
            let mut fields = Vec::new();
            self.skip_ws();
            if !self.consume("}") {
                loop {
                    self.skip_ws();
                    let field_start = self.pos;
                    let name = self.parse_ident().ok_or_else(|| ParseError {
                        message: "expected record field name".to_string(),
                        span: Span::new(self.pos, self.pos),
                    })?;
                    self.skip_ws();
                    self.expect(":")?;
                    self.skip_ws();
                    let value = self.parse_expr()?;
                    fields.push(RecordField {
                        name,
                        value,
                        span: Span::new(field_start, self.pos),
                    });
                    self.skip_ws();
                    if self.consume(",") {
                        continue;
                    }
                    self.expect("}")?;
                    break;
                }
            }
            return Ok(Expr::Record {
                fields,
                span: Span::new(start, self.pos),
            });
        }

        if let Some(s) = self.parse_string()? {
            return Ok(Expr::String {
                value: s,
                span: Span::new(start, self.pos),
            });
        }

        if let Some(n) = self.parse_i64() {
            return Ok(Expr::Number {
                value: n,
                span: Span::new(start, self.pos),
            });
        }

        if self.consume("_") {
            if matches!(self.peek(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
                // `_name` should stay an identifier
                while matches!(self.peek(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
                    self.pos += 1;
                }
                return Ok(Expr::Ident {
                    name: self.src[start..self.pos].to_string(),
                    span: Span::new(start, self.pos),
                });
            }
            return Ok(Expr::Placeholder {
                span: Span::new(start, self.pos),
            });
        }

        if let Some(name) = self.parse_ident() {
            return Ok(Expr::Ident {
                name,
                span: Span::new(start, self.pos),
            });
        }

        Err(ParseError {
            message: "expected expression".to_string(),
            span: Span::new(self.pos, self.pos),
        })
    }

    fn parse_string(&mut self) -> Result<Option<String>, ParseError> {
        if !self.consume("\"") {
            return Ok(None);
        }
        let mut out = String::new();
        let mut escaped = false;
        while let Some(c) = self.peek() {
            self.pos += c.len_utf8();
            if escaped {
                match c {
                    '"' => out.push('"'),
                    '\\' => out.push('\\'),
                    '/' => out.push('/'),
                    'b' => out.push('\u{0008}'),
                    'f' => out.push('\u{000C}'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    _ => {
                        return Err(ParseError {
                            message: format!("unsupported escape: \\{c}"),
                            span: Span::new(self.pos - 1, self.pos),
                        })
                    }
                }
                escaped = false;
                continue;
            }
            match c {
                '\\' => escaped = true,
                '"' => return Ok(Some(out)),
                _ => out.push(c),
            }
        }
        Err(ParseError {
            message: "unterminated string literal".to_string(),
            span: Span::new(self.pos, self.pos),
        })
    }

    fn parse_i64(&mut self) -> Option<i64> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        let digits_start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == digits_start {
            self.pos = start;
            return None;
        }
        self.src[start..self.pos].parse::<i64>().ok().or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_ident(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.pos;
        let first = self.peek()?;
        if !(first.is_ascii_alphabetic() || first == '_') {
            return None;
        }
        self.pos += first.len_utf8();
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
        Some(self.src[start..self.pos].to_string())
    }

    fn expect(&mut self, text: &str) -> Result<(), ParseError> {
        if self.consume(text) {
            Ok(())
        } else {
            Err(ParseError {
                message: format!("expected '{text}'"),
                span: Span::new(self.pos, self.pos),
            })
        }
    }

    fn consume(&mut self, text: &str) -> bool {
        if self.src[self.pos..].starts_with(text) {
            self.pos += text.len();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn eof(&self) -> bool {
        self.pos >= self.src.len()
    }
}

trait Spanned {
    fn span(&self) -> Span;
}

impl Spanned for Expr {
    fn span(&self) -> Span {
        match self {
            Expr::Ident { span, .. }
            | Expr::Placeholder { span }
            | Expr::Number { span, .. }
            | Expr::String { span, .. }
            | Expr::Array { span, .. }
            | Expr::Record { span, .. }
            | Expr::FieldAccess { span, .. }
            | Expr::Call { span, .. }
            | Expr::Pipeline { span, .. }
            | Expr::Compose { span, .. }
            | Expr::Inverse { span, .. }
            | Expr::Raw { span, .. } => *span,
        }
    }
}

fn rebase_expr(expr: Expr, offset: usize) -> Expr {
    match expr {
        Expr::Ident { name, span } => Expr::Ident {
            name,
            span: shift(span, offset),
        },
        Expr::Placeholder { span } => Expr::Placeholder {
            span: shift(span, offset),
        },
        Expr::Number { value, span } => Expr::Number {
            value,
            span: shift(span, offset),
        },
        Expr::String { value, span } => Expr::String {
            value,
            span: shift(span, offset),
        },
        Expr::Array { items, span } => Expr::Array {
            items: items.into_iter().map(|e| rebase_expr(e, offset)).collect(),
            span: shift(span, offset),
        },
        Expr::Record { fields, span } => Expr::Record {
            fields: fields
                .into_iter()
                .map(|f| RecordField {
                    name: f.name,
                    value: rebase_expr(f.value, offset),
                    span: shift(f.span, offset),
                })
                .collect(),
            span: shift(span, offset),
        },
        Expr::FieldAccess { expr, field, span } => Expr::FieldAccess {
            expr: Box::new(rebase_expr(*expr, offset)),
            field,
            span: shift(span, offset),
        },
        Expr::Call { callee, args, span } => Expr::Call {
            callee: Box::new(rebase_expr(*callee, offset)),
            args: args
                .into_iter()
                .map(|arg| match arg {
                    CallArg::Positional(e) => CallArg::Positional(rebase_expr(e, offset)),
                    CallArg::Named { name, value, span } => CallArg::Named {
                        name,
                        value: rebase_expr(value, offset),
                        span: shift(span, offset),
                    },
                })
                .collect(),
            span: shift(span, offset),
        },
        Expr::Pipeline { input, stages, span } => Expr::Pipeline {
            input: Box::new(rebase_expr(*input, offset)),
            stages: stages.into_iter().map(|e| rebase_expr(e, offset)).collect(),
            span: shift(span, offset),
        },
        Expr::Compose { left, right, span } => Expr::Compose {
            left: Box::new(rebase_expr(*left, offset)),
            right: Box::new(rebase_expr(*right, offset)),
            span: shift(span, offset),
        },
        Expr::Inverse { expr, span } => Expr::Inverse {
            expr: Box::new(rebase_expr(*expr, offset)),
            span: shift(span, offset),
        },
        Expr::Raw { text, span } => Expr::Raw {
            text,
            span: shift(span, offset),
        },
    }
}

fn shift(span: Span, offset: usize) -> Span {
    Span::new(span.start + offset, span.end + offset)
}
