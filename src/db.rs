use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::Context;

use crate::{
    page::{Page, TableLeafPage}, record::Value, sql::{
        parser::{self, Stmt},
        scanner,
    }, utils::read_be_word_at
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
    pub fn execute(&mut self, sql: &str) {
        let mut scanner = scanner::Scanner::new(sql.to_string());
        let tokens = scanner.scan_tokens();
        let mut parser = parser::Parser::new(tokens.clone());
        let stmts = parser.parse().unwrap();
        for stmt in stmts {
            match stmt {
                Stmt::Select(columns, from, where_clause) => {
                    if let Some(table_ref) = from {
                      
                      let root_page = self.read_first_page().unwrap();
                      match root_page {
                          Page::TableLeaf(leaf_page) => {
                            let mut table_index = None;
                              for cell in &leaf_page.cells {
                                 if let Value::String(name) = &cell.record.body.get(2).unwrap().value {
                                    if name.to_lowercase() == table_ref.name.to_lowercase() {
                                        table_index = cell.record.body.get(3).map(|col| col.value.clone());
                                        break;
                                    }
                                 }
                              }
                              if let Value::I8(index) = table_index.unwrap() {
                                let page = self.read_page(index as usize).unwrap();
                                match page {
                                    Page::TableLeaf(table_page) => {
                                        println!("{:?}", table_page.cells.len());
                                    }
                                    _ => {}
                                }
                              }
                          }
                          _ => {}
                      }
                    }
                }
            }
        }
    }
    fn read_page(&mut self, page_num: usize) -> anyhow::Result<Page> {
        self.pager.read_page(page_num).map(|page| page.clone())
    }
    fn read_first_page(&mut self) -> anyhow::Result<Page> {
        self.read_page(1)
    }
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
