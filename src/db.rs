use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::Context;

use crate::{
    page::{Page, TableInteriorPage, TableLeafPage},
    record::Value,
    sql::{
        parser::{self, Expr, Literal, Stmt},
        scanner,
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
    pub fn execute_sql(&mut self, sql: &str) -> anyhow::Result<()> {
        let mut scanner = scanner::Scanner::new(sql.to_string());
        let tokens = scanner.scan_tokens();
        let mut parser = parser::Parser::new(tokens.clone());
        let stmts = parser.parse().unwrap();
        for stmt in stmts {
            match stmt {
                Stmt::Select(columns, from, where_clause) => {
                    if let Some(table_ref) = from {
                        if let Some(schema) = self.get_table_schema(&table_ref.name)? {
                            self.execute_page(
                                schema.root_page as usize,
                                &columns,
                                &schema,
                                where_clause,
                            )?;
                        }
                    }
                }
            }
        }
        anyhow::Ok(())
    }

    fn execute_page(
        &mut self,
        page_num: usize,
        columns: &[Expr],
        schema: &TableSchema,
        where_clause: Option<Expr>,
    ) -> anyhow::Result<()> {
        let page = self.read_page(page_num)?;
        match page {
            Page::TableLeaf(table_page) => {
                if columns.len() == 1 {
                    if let Expr::FunctionCall(_, _) = &columns[0] {
                        let value = self.execute_func(&columns[0], &table_page);
                        println!("{}", value);
                    } else {
                        self.execute_query(columns, &table_page, schema, where_clause.clone());
                    }
                } else {
                    self.execute_query(columns, &table_page, schema, where_clause.clone());
                }
            }

            // TODO: 
            Page::TableInterior(interior_page) => {
                for cell in &interior_page.cells {
                    self.execute_page(
                        cell.left_child as usize,
                        columns,
                        schema,
                        where_clause.clone(),
                    )?;
                }
                let right_most_page = interior_page.header.get_right_most_point();
                self.execute_page(
                    right_most_page as usize,
                    columns,
                    schema,
                    where_clause.clone(),
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn execute_func(&mut self, func: &Expr, page: &TableLeafPage) -> String {
        match func {
            Expr::FunctionCall(name, args) => {
                if let Expr::Identifier(func_name) = name.as_ref() {
                    match func_name.as_str() {
                        "count" => {
                            if args.is_empty() {
                                page.cells.len().to_string()
                            } else {
                                let mut count = 0;
                                for cell in &page.cells {
                                    if let Some(body) = cell.record.body.get(0) {
                                        if body.value != Value::Null {
                                            count += 1;
                                        }
                                    }
                                }
                                count.to_string()
                            }
                        }
                        _ => "".to_string(),
                    }
                } else {
                    "".to_string()
                }
            }
            _ => "".to_string(),
        }
    }
    fn execute_query(
        &mut self,
        columns: &[Expr],
        table_page: &TableLeafPage,
        schema: &TableSchema,
        where_clause: Option<Expr>,
    ) {
        for cell in &table_page.cells {
            let mut row_map = HashMap::new();
            for (i, column) in schema.columns.iter().enumerate() {
                let value = if let Some(body) = cell.record.body.get(i) {
                    body.value.to_string()
                } else {
                    "Null".to_string()
                };
                row_map.insert(column.name.clone(), value);
            }
            let mut should_print = true;
            if let Some(where_expr) = &where_clause {
                match where_expr {
                    Expr::BinaryOp(column, op, value) => {
                        let column_name = match column.as_ref() {
                            Expr::Identifier(name) => name,
                            _ => continue,
                        };
                        let value_str = match value.as_ref() {
                            Expr::Literal(Literal::String(s)) => s.clone(),
                            Expr::Literal(Literal::Number(n)) => n.to_string(),
                            _ => continue,
                        };
                        should_print = self.check(column_name, &value_str, &op.lexeme, &row_map);
                    }
                    _ => {}
                }
            }

            if should_print {
                let mut values = Vec::new();
              
                for column in columns {
                    match column {
                        Expr::Identifier(name) => {
                            let null_str = "Null".to_string();
                            let value = row_map.get(name).unwrap_or(&null_str);
                            values.push(value.clone());
                        }
                        Expr::FunctionCall(_, _) => {
                            let value = self.execute_func(column, table_page);
                            values.push(value);
                        }
                        _ => {}
                    }
                }
                println!("{}", values.join("|"));
            }
        }
    }

    fn check(
        &mut self,
        where_column: &str,
        where_value: &str,
        where_op: &str,
        row_map: &HashMap<String, String>,
    ) -> bool {
        match row_map.get(where_column) {
            Some(value) => match where_op {
                "=" => value == where_value,
                "!=" => value != where_value,
                "<" => value.parse::<i64>().unwrap() < where_value.parse::<i64>().unwrap(),
                ">" => value.parse::<i64>().unwrap() > where_value.parse::<i64>().unwrap(),
                "<=" => value.parse::<i64>().unwrap() <= where_value.parse::<i64>().unwrap(),
                ">=" => value.parse::<i64>().unwrap() >= where_value.parse::<i64>().unwrap(),
                _ => false,
            },
            None => false,
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
