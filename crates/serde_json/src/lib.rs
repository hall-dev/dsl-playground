use std::collections::BTreeMap;

pub type Map = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq)]
pub struct Number(i64);

impl Number {
    pub fn as_i64(&self) -> Option<i64> {
        Some(self.0)
    }
}

impl From<i64> for Number {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Value>),
    Object(Map),
}

#[derive(Debug, Clone)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Error {}

pub fn from_str(input: &str) -> Result<Value, Error> {
    let mut p = JsonP {
        b: input.as_bytes(),
        i: 0,
    };
    let value = p.value().map_err(Error)?;
    p.ws();
    if p.i != p.b.len() {
        return Err(Error("trailing json".to_string()));
    }
    Ok(value)
}

pub fn from_slice(input: &[u8]) -> Result<Value, Error> {
    let s = std::str::from_utf8(input).map_err(|e| Error(e.to_string()))?;
    from_str(s)
}

pub fn to_string(value: &Value) -> Result<String, Error> {
    Ok(stringify_json(value))
}

pub fn to_vec(value: &Value) -> Result<Vec<u8>, Error> {
    Ok(stringify_json(value).into_bytes())
}

fn stringify_json(j: &Value) -> String {
    match j {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.0.to_string(),
        Value::String(s) => format!(
            "\"{}\"",
            s.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        ),
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(stringify_json).collect::<Vec<_>>().join(",")
        ),
        Value::Object(o) => format!(
            "{{{}}}",
            o.iter()
                .map(|(k, v)| format!("\"{}\":{}", k.replace('"', "\\\""), stringify_json(v)))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

struct JsonP<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> JsonP<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn value(&mut self) -> Result<Value, String> {
        self.ws();
        if self.i >= self.b.len() {
            return Err("eof".to_string());
        }
        match self.b[self.i] {
            b'n' => {
                self.expect(b"null")?;
                Ok(Value::Null)
            }
            b't' => {
                self.expect(b"true")?;
                Ok(Value::Bool(true))
            }
            b'f' => {
                self.expect(b"false")?;
                Ok(Value::Bool(false))
            }
            b'"' => Ok(Value::String(self.string()?)),
            b'[' => self.array(),
            b'{' => self.object(),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err("bad json value".to_string()),
        }
    }

    fn expect(&mut self, s: &[u8]) -> Result<(), String> {
        if self.b.get(self.i..self.i + s.len()) == Some(s) {
            self.i += s.len();
            Ok(())
        } else {
            Err("bad token".to_string())
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.i += 1;
        let mut o = String::new();
        while self.i < self.b.len() {
            let c = self.b[self.i];
            self.i += 1;
            if c == b'"' {
                return Ok(o);
            }
            if c == b'\\' {
                if self.i >= self.b.len() {
                    return Err("bad escape".to_string());
                }
                let e = self.b[self.i];
                self.i += 1;
                o.push(match e {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'n' => '\n',
                    b't' => '\t',
                    _ => return Err("bad escape".to_string()),
                });
            } else {
                o.push(c as char)
            }
        }
        Err("unterminated string".to_string())
    }

    fn number(&mut self) -> Result<Value, String> {
        let s = self.i;
        if self.b[self.i] == b'-' {
            self.i += 1;
        }
        while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
            self.i += 1;
        }
        let n = std::str::from_utf8(&self.b[s..self.i])
            .map_err(|_| "utf8".to_string())?
            .parse::<i64>()
            .map_err(|_| "num".to_string())?;
        Ok(Value::Number(n.into()))
    }

    fn array(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut out = vec![];
        loop {
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b']' {
                self.i += 1;
                return Ok(Value::Array(out));
            }
            out.push(self.value()?);
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b',' {
                self.i += 1;
                continue;
            }
            if self.i < self.b.len() && self.b[self.i] == b']' {
                self.i += 1;
                return Ok(Value::Array(out));
            }
            return Err("bad array".to_string());
        }
    }

    fn object(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut out = BTreeMap::new();
        loop {
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b'}' {
                self.i += 1;
                return Ok(Value::Object(out));
            }
            let key = self.string()?;
            self.ws();
            if self.i >= self.b.len() || self.b[self.i] != b':' {
                return Err("bad object".to_string());
            }
            self.i += 1;
            out.insert(key, self.value()?);
            self.ws();
            if self.i < self.b.len() && self.b[self.i] == b',' {
                self.i += 1;
                continue;
            }
            if self.i < self.b.len() && self.b[self.i] == b'}' {
                self.i += 1;
                return Ok(Value::Object(out));
            }
            return Err("bad object".to_string());
        }
    }
}

#[macro_export]
macro_rules! json {
    ($($tt:tt)+) => {
        $crate::from_str(stringify!($($tt)+)).expect("valid json literal")
    };
}
