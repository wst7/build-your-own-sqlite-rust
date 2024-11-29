use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Ok};

use crate::{
    page::{Page, TableInteriorPage, TableLeafPage},
    record::Value,
    sql::{
        parser::{self, Expr, Literal, Stmt},
        scanner, token::TokenType,
    },
    utils::read_be_word_at,
};

pub const HEADER_SIZE: usize = 100;
const HEADER_PREFIX: &[u8] = b"SQLite format 3\0";
const HEADER_PAGE_SIZE_OFFSET: usize = 16;
const PAGE_MAX_SIZE: u32 = 65_536;

#[derive(Debug, Clone)]
pub struct DbHeader {
    pub page_size: u32,
}
impl DbHeader {
    pub fn parse(buffer: &[u8]) -> anyhow::Result<Self> {
        if !buffer.starts_with(HEADER_PREFIX) {
            let prefix = &buffer[..HEADER_PREFIX.len()];
            anyhow::bail!("Invalid header prefix: {:?}", prefix);
        }
        let page_size_raw = read_be_word_at(buffer, HEADER_PAGE_SIZE_OFFSET);
        let page_size = match page_size_raw {
            1 => PAGE_MAX_SIZE,
            n if n.is_power_of_two() => n as u32,
            _ => anyhow::bail!("page size is not a power of 2: {}", page_size_raw),
        };
        Ok(DbHeader { page_size })
    }
}

pub struct Db {
    pub header: DbHeader,
    pub pager: Pager,
}

impl Db {
    pub fn from_file(filename: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut file = File::open(filename).context("open db file")?;
        let mut header_buffer = [0; HEADER_SIZE];
        file.read_exact(&mut header_buffer)
            .context("read db header")?;
        let header = DbHeader::parse(&header_buffer)?;
        let pager = Pager::new(file, header.page_size as usize);
        Ok(Db { header, pager })
    }
    pub fn execute_sql(&mut self, sql: &str) -> anyhow::Result<Vec<Vec<Vec<String>>>> {
        let mut scanner = scanner::Scanner::new(sql.to_string());
        let tokens = scanner.scan_tokens();
        let mut parser = parser::Parser::new(tokens.clone());
        let stmts = parser.parse().unwrap();
        let mut result = Vec::new();
        for stmt in stmts {
            match stmt {
                Stmt::Select(columns, from, where_clause) => {
                    if let Some(table_ref) = from {
                        if let Some(schema) = self.get_table_schema(&table_ref.name)? {
                            let page = self.read_page(schema.root_page as usize)?;
                            let rows = match page {
                                Page::TableLeaf(leaf_page) => self.query_leaf_page(&leaf_page, &columns, &schema, &where_clause),
                                Page::TableInterior(interior_page) => self.query_interior_page(&interior_page, &columns, &schema, &where_clause),
                                _ => anyhow::bail!("Unknown page type in query: {:?}", page.get_page_type()),
                            }?;
                        
                            result.push(rows);
                        }
                    }
                }
            }
        }
        anyhow::Ok(result)
    }

    fn query_leaf_page(&mut self, leaf_page: &TableLeafPage, columns: &[Expr], schema: &TableSchema, where_clause: &Option<Expr>) -> anyhow::Result<Vec<Vec<String>>> {
        let mut result = Vec::new();
        for cell in &leaf_page.cells {

            let mut row_map = HashMap::new();
            for (column, record_body) in schema.columns.iter().zip(cell.record.body.iter()) {
                let key = column.name.clone();
                let value = record_body.value.to_string();
                row_map.insert(key, value);
            }
            if !self.where_clause_matches(where_clause, &row_map) {
                continue;
            }
            let mut row = Vec::new();

            for column in columns {
                match column {
                    Expr::Identifier(name) => {
                        if let Some(value) = row_map.get(name) {
                            row.push(value.clone());
                        } else {
                            row.push("NULL".to_string());
                        }
                    },
                    Expr::FunctionCall(name, args) => {
                        if let Expr::Identifier(func_name) = name.as_ref() {
                            match func_name.as_str() {
                                "count" => {
                                    let count = leaf_page.cells.len() as i64;
                                    row.push(count.to_string());
                                    return Ok(vec![row]);
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                
            }
            result.push(row);
        }
        Ok(result)
    }
    fn query_interior_page(&mut self, interior_page: &TableInteriorPage, columns: &[Expr], schema: &TableSchema, where_clause: &Option<Expr>) -> anyhow::Result<Vec<Vec<String>>> {
        let mut result = Vec::new();
        for cell in &interior_page.cells {
            let page = self.read_page(cell.left_child as usize)?;
            match page {
                Page::TableLeaf(leaf_page) => {
                    let mut rows = self.query_leaf_page(&leaf_page, columns, schema, where_clause)?;
                    result.append(&mut rows);
                }
                Page::TableInterior(interior_page) => {
                    let mut rows = self.query_interior_page(&interior_page, columns, schema, where_clause)?;
                    result.append(&mut rows);
                }
                _ => {}
            }
        }
        Ok(result)
    }

    fn where_clause_matches(&mut self, where_clause: &Option<Expr>, row_map: &HashMap<String, String>) -> bool {
        match where_clause {
            Some(expr) => self.check(expr, row_map),
            None => true,
        }
    }
    fn check(
        &mut self,
        where_expr: &Expr,
        row_map: &HashMap<String, String>,
    ) -> bool {
        match where_expr {
            Expr::BinaryOp(left, op, right) => {
                let left = if let Expr::Identifier(name) = left.as_ref() {
                    row_map.get(name).unwrap().to_string()
                } else {
                    "".to_string()
                };
                let right = match right.as_ref() {
                    Expr::Identifier(name) => row_map.get(name).unwrap().to_string(),
                    Expr::Literal(literal) => match literal {
                        Literal::String(s) => s.to_string(),
                        Literal::Number(n) => n.to_string(),
                        Literal::Boolean(b) => b.to_string(),
                        Literal::Null => "NULL".to_string(),
                    },
                    _ => "".to_string(),
                };
 
                match op.token_type {
                    TokenType::Equal => left == right,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn read_page(&mut self, page_num: usize) -> anyhow::Result<Page> {
        self.pager.read_page(page_num).map(|page| page.clone())
    }
    fn read_first_page(&mut self) -> anyhow::Result<Page> {
        self.read_page(1)
    }

    pub fn get_schemas(&mut self) -> anyhow::Result<Vec<TableSchema>> {
        let first_page = self.read_first_page()?;
        let mut schemas = Vec::new();
        if let Page::TableLeaf(page) = first_page {
            for cell in page.cells {
                // 0: schema_type
                // 1: schema_name
                // 2: table_name
                // 3: rootpage
                // 4: sql

                if let Some(schema_type_body) = cell.record.body.get(0) {
                    if let Value::String(schema_type) = &schema_type_body.value {
                        if schema_type == "table" {
                            let name = match &cell.record.body.get(1).unwrap().value {
                                Value::String(name) => name.clone(),
                                _ => continue,
                            };
                            let root_page = match &cell.record.body.get(3).unwrap().value {
                                Value::I64(n) => *n as i8,
                                _ => continue,
                            };
                            let sql = match &cell.record.body.get(4).unwrap().value {
                                Value::String(sql) => sql.clone(),
                                _ => continue,
                            };

                            let columns = parse_create_table_sql(&sql)?;
                            schemas.push(TableSchema {
                                name,
                                sql,
                                root_page,
                                columns,
                            });
                        }
                    }
                }
            }
        }
        Ok(schemas)
    }

    pub fn get_table_schema(&mut self, table_name: &str) -> anyhow::Result<Option<TableSchema>> {
        let schemas = self.get_schemas()?;
        // println!("schemas: {:#?}", schemas);
        let schema = schemas
            .into_iter()
            .find(|s| s.name.to_lowercase() == table_name.to_lowercase());
        anyhow::Ok(schema)
    }
}

#[derive(Debug)]
pub struct TableSchema {
    name: String,
    sql: String,
    root_page: i8,
    columns: Vec<Column>,
}
#[derive(Debug)]
pub struct Column {
    name: String,
    type_name: String,
}
fn parse_create_table_sql(sql: &str) -> anyhow::Result<Vec<Column>> {
    let mut columns = vec![];
    let sql = sql.to_lowercase();
    if let Some(start) = sql.find("(") {
        if let Some(end) = sql.rfind(")") {
            let column_defs = &sql[start + 1..end];
            for column_def in column_defs.split(",") {
                let parts = column_def.split_whitespace().collect::<Vec<&str>>();
                if parts.len() >= 2 {
                    columns.push(Column {
                        name: parts[0].to_string(),
                        type_name: parts[1].to_string(),
                    });
                }
            }
        }
    }
    anyhow::Ok(columns)
}
pub struct Pager<I: std::fmt::Debug + Read + Seek = std::fs::File> {
    input: I,
    page_size: usize,
    pages: HashMap<usize, Page>,
}

impl<I: Read + Seek + std::fmt::Debug> Pager<I> {
    pub fn new(input: I, page_size: usize) -> Self {
        Self {
            input,
            page_size,
            pages: HashMap::new(),
        }
    }
    pub fn read_page(&mut self, page_num: usize) -> anyhow::Result<&Page> {
        if self.pages.contains_key(&page_num) {
            return Ok(self.pages.get(&page_num).unwrap());
        }
        let page = self.load_page(page_num)?;
        self.pages.insert(page_num, page.clone());
        Ok(self.pages.get(&page_num).unwrap())
    }
    fn load_page(&mut self, page_num: usize) -> anyhow::Result<Page> {
        let offset = page_num.saturating_sub(1) * self.page_size;
        self.input
            .seek(SeekFrom::Start(offset as u64))
            .context("seek to page start")?;
        let mut buffer = vec![0; self.page_size];
        self.input.read_exact(&mut buffer).context("read page")?;
        Ok(Page::parse(&buffer, page_num)?)
    }
}
