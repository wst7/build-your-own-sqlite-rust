#[derive(Debug, PartialEq, Clone)]
pub enum TokenType {
    // Single-character tokens
    LeftParen, RightParen, Comma, Dot, Semicolon, Star,
    
    // Literals
    Identifier, String, Number,
    
    // Keywords
    Select, From, Where, And, Or,
    Insert, Into, Values,
    Create, Table,
    Delete, Update, Set, As,
    
    EOF
}

#[derive(Debug, Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub lexeme: String,
    pub literal: Option<String>,
    pub line: usize,
}

impl Token {
    pub fn new(token_type: TokenType, lexeme: String, literal: Option<String>, line: usize) -> Self {
        Token {
            token_type,
            lexeme,
            literal,
            line,
        }
    }
}