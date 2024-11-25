use std::{collections::HashMap, sync::LazyLock};

use super::token::TokenType;


static KEYWORDS: LazyLock<HashMap<String, TokenType>> = LazyLock::new(|| {
    let map = HashMap::from([
        ("SELECT".to_string(), TokenType::Select),
        ("FROM".to_string(), TokenType::From),
        ("WHERE".to_string(), TokenType::Where),
        ("AND".to_string(), TokenType::And),
        ("OR".to_string(), TokenType::Or),
        ("INSERT".to_string(), TokenType::Insert),
        ("INTO".to_string(), TokenType::Into),
        ("VALUES".to_string(), TokenType::Values),
        ("CREATE".to_string(), TokenType::Create),
        ("TABLE".to_string(), TokenType::Table),
        ("DELETE".to_string(), TokenType::Delete),
        ("UPDATE".to_string(), TokenType::Update),
        ("SET".to_string(), TokenType::Set),
    ]);
    map
});

pub fn get(text: &str) -> Option<TokenType> {
    let keyword = text.to_uppercase();
    KEYWORDS.get(&keyword).cloned()
}