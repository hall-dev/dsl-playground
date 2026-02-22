#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Stmt>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Binding {
        name: String,
        expr: Expr,
        span: Span,
    },
    Pipeline {
        expr: Expr,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Ident { name: String, span: Span },
    Placeholder { span: Span },
    Number { value: i64, span: Span },
    String { value: String, span: Span },
    Array { items: Vec<Expr>, span: Span },
    Record { fields: Vec<RecordField>, span: Span },
    FieldAccess {
        expr: Box<Expr>,
        field: String,
        span: Span,
    },
    Call {
        callee: Box<Expr>,
        args: Vec<CallArg>,
        span: Span,
    },
    Pipeline {
        input: Box<Expr>,
        stages: Vec<Expr>,
        span: Span,
    },
    Compose {
        left: Box<Expr>,
        right: Box<Expr>,
        span: Span,
    },
    Inverse {
        expr: Box<Expr>,
        span: Span,
    },
    Raw { text: String, span: Span },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallArg {
    Positional(Expr),
    Named {
        name: String,
        value: Expr,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordField {
    pub name: String,
    pub value: Expr,
    pub span: Span,
}
