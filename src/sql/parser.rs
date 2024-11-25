use super::token::{Token, TokenType};

#[derive(Debug)]
pub enum Stmt {
    // columns, from, where
    Select(Vec<Expr>, Option<TableReference>, Option<Expr>),
}

// #[derive(Debug)]
// pub struct SelectStmt {
//     pub columns: Vec<Expr>,
//     pub from: Option<TableReference>,
//     pub where_clause: Option<Expr>,
// }

#[derive(Debug)]
pub struct TableReference {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug)]
pub enum Expr {
    Identifier(String),
    Literal(Literal),
    BinaryOp(Box<Expr>, Token, Box<Expr>),
    FunctionCall(Box<Expr>, Vec<Expr>),
    Wildcard,
    Aliased(Box<Expr>, String),
}

#[derive(Debug)]
pub enum Literal {
    String(String),
    Number(f64),
    Boolean(bool),
    Null,
}

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, current: 0 }
    }
    pub fn parse(&mut self) -> anyhow::Result<Vec<Stmt>> {
        let mut stmts = Vec::new();
        while !self.is_at_end() {
            stmts.push(self.parse_stmt()?);
        }
        Ok(stmts)
    }
    fn parse_stmt(&mut self) -> anyhow::Result<Stmt> {
        if self.matches(&[TokenType::Select]) {
            return Ok(self.select_stmt()?);
        }
        todo!()
    }
    fn select_stmt(&mut self) -> anyhow::Result<Stmt> {
        let columns = self.select_list()?;

        self.consume(TokenType::From, "Expected 'FROM' after select columns")?;

        let from = Some(self.table_reference()?);

        let where_clause = if self.matches(&[TokenType::Where]) {
            Some(self.expression()?)
        } else {
            None
        };
        Ok(Stmt::Select(columns, from, where_clause))
    }
    fn select_list(&mut self) -> anyhow::Result<Vec<Expr>> {
        let mut columns = Vec::new();
        loop {
            columns.push(self.expression()?);
            if !self.matches(&[TokenType::Comma]) {
                break;
            }
        }
        Ok(columns)
    }
    fn table_reference(&mut self) -> anyhow::Result<TableReference> {
        let name = self
            .consume(TokenType::Identifier, "Expected table name")?
            .lexeme
            .clone();
        let alias = if self.matches(&[TokenType::As]) {
            Some(
                self.consume(TokenType::Identifier, "Expected table alias")?
                    .lexeme
                    .clone(),
            )
        } else {
            None
        };
        Ok(TableReference { name, alias })
    }
    fn expression(&mut self) -> anyhow::Result<Expr> {
        // function call
        if self.check(&TokenType::Identifier) {
            if self.peek_next().token_type == TokenType::LeftParen {
                return self.function_call();
            }
        }
        self.primary()
    }
    fn function_call(&mut self) -> anyhow::Result<Expr> {
        let name = self.advance().lexeme.clone();
        self.consume(TokenType::LeftParen, "Expected '(' after function name")?;
        let mut args = Vec::new();

        if self.matches(&[TokenType::Star]) {
            args.push(Expr::Wildcard);
        } else {
            loop {
                args.push(self.expression()?);
                if !self.matches(&[TokenType::Comma]) {
                    break;
                }
            }
        }
        self.consume(
            TokenType::RightParen,
            "Expected ')' after function arguments",
        )?;
        Ok(Expr::FunctionCall(Box::new(Expr::Identifier(name)), args))
    }
    fn primary(&mut self) -> anyhow::Result<Expr> {
        if self.matches(&[TokenType::Identifier]) {
            return Ok(Expr::Identifier(self.previous().lexeme.clone()));
        }
        if self.matches(&[TokenType::String]) {
            return Ok(Expr::Literal(Literal::String(
                self.previous().literal.clone().unwrap(),
            )));
        }
        if self.matches(&[TokenType::Number]) {
            let num_str = self.previous().literal.clone().unwrap();
            let number = match num_str.parse::<f64>() {
                Ok(n) => n,
                Err(e) => anyhow::bail!("Invalid number".to_string()),
            };
            return Ok(Expr::Literal(Literal::Number(number)));
        }
        if self.matches(&[TokenType::Star]) {
            return Ok(Expr::Wildcard);
        }
        todo!();
    }
    fn matches(&mut self, types: &[TokenType]) -> bool {
        for t in types {
            if self.check(t) {
                self.advance();
                return true;
            }
        }
        false
    }
    fn check(&mut self, token_type: &TokenType) -> bool {
        if self.is_at_end() {
            return false;
        }
        self.peek().token_type == *token_type
    }
    fn consume(&mut self, token_type: TokenType, message: &str) -> anyhow::Result<&Token> {
        if self.check(&token_type) {
            return Ok(self.advance());
        }
        anyhow::bail!(message.to_string());
    }
    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }
    fn peek_next(&self) -> &Token {
        if self.is_at_end() {
            return &self.tokens[self.current];
        }
        &self.tokens[self.current + 1]
    }
    fn advance(&mut self) -> &Token {
        if !self.is_at_end() {
            self.current += 1;
        }
        self.previous()
    }
    fn previous(&self) -> &Token {
        &self.tokens[self.current - 1]
    }
    fn is_at_end(&self) -> bool {
        self.peek().token_type == TokenType::EOF
    }
}
