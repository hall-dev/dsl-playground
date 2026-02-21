#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Stmt>,
}

#[derive(Debug, Clone)]
pub enum Stmt {
    Binding { name: String, expr: Expr },
    Pipeline { expr: Expr },
}

#[derive(Debug, Clone)]
pub enum Expr {
    Ident(String),
    Number(i64),
    String(String),
    Pipeline { input: Box<Expr>, stages: Vec<Expr> },
    Call { name: String, args: Vec<Expr> },
    Compose { left: Box<Expr>, right: Box<Expr> },
    Inverse(Box<Expr>),
    Raw(String),
}

#[derive(Debug, Clone)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ParseError {}

pub fn parse_program(input: &str) -> Result<Program, ParseError> {
    let mut statements = Vec::new();
    for raw in input.split(';') {
        let stmt = raw.trim();
        if stmt.is_empty() {
            continue;
        }
        if let Some((name, expr)) = stmt.split_once(":=") {
            statements.push(Stmt::Binding {
                name: name.trim().to_string(),
                expr: parse_expr(expr.trim())?,
            });
        } else {
            statements.push(Stmt::Pipeline {
                expr: parse_expr(stmt)?,
            });
        }
    }
    Ok(Program { statements })
}

fn parse_expr(input: &str) -> Result<Expr, ParseError> {
    let parts = split_top_level(input, "|>");
    if parts.len() > 1 {
        let mut it = parts.into_iter();
        let first = parse_atomic(it.next().expect("non-empty"))?;
        let stages = it.map(parse_atomic).collect::<Result<Vec<_>, _>>()?;
        return Ok(Expr::Pipeline {
            input: Box::new(first),
            stages,
        });
    }
    parse_atomic(input)
}

fn parse_atomic(input: &str) -> Result<Expr, ParseError> {
    let s = input.trim();
    if let Some(rest) = s.strip_prefix('~') {
        return Ok(Expr::Inverse(Box::new(parse_atomic(rest)?)));
    }
    let composed = split_top_level(s, ">>");
    if composed.len() > 1 {
        let mut acc = parse_atomic(composed[0])?;
        for p in composed.iter().skip(1) {
            acc = Expr::Compose {
                left: Box::new(acc),
                right: Box::new(parse_atomic(p)?),
            };
        }
        return Ok(acc);
    }
    if s.starts_with('"') && s.ends_with('"') {
        return Ok(Expr::String(unescape(&s[1..s.len() - 1])?));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Ok(Expr::Number(n));
    }
    if let Some(idx) = s.find('(') {
        if s.ends_with(')') {
            let name = s[..idx].trim().to_string();
            let inner = &s[idx + 1..s.len() - 1];
            let args = if inner.trim().is_empty() {
                vec![]
            } else {
                split_top_level(inner, ",")
                    .into_iter()
                    .map(parse_expr)
                    .collect::<Result<Vec<_>, _>>()?
            };
            return Ok(Expr::Call { name, args });
        }
    }
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Ok(Expr::Ident(s.to_string()));
    }
    Ok(Expr::Raw(s.to_string()))
}

fn unescape(s: &str) -> Result<String, ParseError> {
    let mut out = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars
            .next()
            .ok_or_else(|| ParseError("incomplete escape".to_string()))?
        {
            'n' => out.push('\n'),
            't' => out.push('\t'),
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            other => return Err(ParseError(format!("unsupported escape: {other}"))),
        }
    }
    Ok(out)
}

fn split_top_level<'a>(input: &'a str, sep: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;
    let chars: Vec<char> = input.chars().collect();
    let sep_chars: Vec<char> = sep.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            '"' => {
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    if chars[i] == '\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        if depth == 0
            && i + sep_chars.len() <= chars.len()
            && chars[i..i + sep_chars.len()] == sep_chars[..]
        {
            out.push(input[start..i].trim());
            i += sep_chars.len();
            start = i;
            continue;
        }
        i += 1;
    }
    out.push(input[start..].trim());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_program() {
        assert_eq!(
            parse_program("x := input.json(\"x\") |> ~json; x |> map(_ + 1);")
                .unwrap()
                .statements
                .len(),
            2
        );
    }
    #[test]
    fn parses_compose() {
        assert!(matches!(
            parse_program("c := base64 >> ~base64;").unwrap().statements[0],
            Stmt::Binding { .. }
        ));
    }
}
