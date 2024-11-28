use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::Context;

use crate::{
    page::{Page, TableLeafPage},
    record::Value,
    sql::{
        parser::{self, Expr, Stmt},
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
    pub fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        let mut scanner = scanner::Scanner::new(sql.to_string());
        let tokens = scanner.scan_tokens();
        let mut parser = parser::Parser::new(tokens.clone());
        let stmts = parser.parse().unwrap();
        for stmt in stmts {
            match stmt {
                Stmt::Select(columns, from, where_clause) => {
                    if let Some(table_ref) = from {
                        if let Some(schema) = self.get_table_schema(&table_ref.name)? {
                            let page = self.read_page(schema.root_page as usize)?;
                            match page {
                                Page::TableLeaf(table_page) => {
                                    if columns.len() == 1 {
                                        if let Expr::FunctionCall(iden, args) = &columns[0] {
                                            self.execute_func(&columns[0], &table_page);
                                        } else {
                                            self.execute_query(
                                                &columns,
                                                &table_page,
                                                &schema,
                                            );
                                        }
                                    } else {
                                        self.execute_query(
                                            &columns,
                                            &table_page,
                                            &schema,
                                        );
                                    }
                                }

                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        anyhow::Ok(())
    }
    fn execute_func(&mut self, func: &Expr, page: &TableLeafPage) {
        match func {
            Expr::FunctionCall(iden, args) => {
                if let Expr::Identifier(name) = iden.as_ref() {
                    if name.to_lowercase() == "count"
                        && args.len() == 1
                        && args[0] == Expr::Wildcard
                    {
                        println!("{}", page.cells.len());
                    }
                }
            }
            _ => {}
        }
    }
    fn execute_query(
        &mut self,
        columns: &[Expr],
        table_page: &TableLeafPage,
        schema: &TableSchema,
    ) {
        let column_names: Vec<String> = columns
            .iter()
            .map(|column| match column {
                Expr::Identifier(name) => name.clone(),
                _ => "".to_string(),
            })
            .collect();
            
        for cell in &table_page.cells {
            let mut row_map = HashMap::<String, String>::new();
            let mut row = Vec::new();
            for (column, record_body) in schema.columns.iter().zip(cell.record.body.iter()) {
              row_map.insert(column.name.clone(), record_body.value.to_string());
                // if column_names.contains(&column.name) {
                //     row.push(record_body.value.to_string());
                // }
            }
            for column_name in column_names.iter() {
                if let Some(value) = row_map.get(column_name) {
                    row.push(value.clone());
                } else {
                    row.push("".to_string());
                }
            }
            println!("{}", row.join("|"));
        }
    }
    fn read_page(&mut self, page_num: usize) -> anyhow::Result<Page> {
        self.pager.read_page(page_num).map(|page| page.clone())
    }
    fn read_first_page(&mut self) -> anyhow::Result<Page> {
        self.read_page(1)
    }

    pub fn get_schemas(&mut self) -> anyhow::Result<Vec<TableSchema>> {
        let root_page = self.read_first_page()?;
        let mut schemas = Vec::new();
        if let Page::TableLeaf(leaf_page) = root_page {
            for cell in leaf_page.cells {
                // Schema table columns:
                // 0: type (table, index, etc)
                // 1: name
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
                                Value::I8(n) => n.clone(),
                                _ => continue,
                            };
                            let sql = match &cell.record.body.get(4).unwrap().value {
                                Value::String(sql) => sql.clone(),
                                _ => continue,
                            };

                            schemas.push(TableSchema {
                                name,
                                columns: parse_create_table_sql(&sql)?,
                                root_page,
                                sql,
                            });
                        }
                    }
                }
            }
        }
        anyhow::Ok(schemas)
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
pub struct Pager<I: Read + Seek = std::fs::File> {
    input: I,
    page_size: usize,
    pages: HashMap<usize, Page>,
}

impl<I: Read + Seek> Pager<I> {
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
