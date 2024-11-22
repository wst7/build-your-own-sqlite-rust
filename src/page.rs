use anyhow::Ok;

use crate::{db::HEADER_SIZE, record::Record, utils::{read_be_word_at, read_varint_at}};


pub const TABLE_LEAF_PAGE_ID: u8 = 0x0d;

const PAGE_LEAF_HEADER_SIZE: usize = 8;
const PAGE_FIRST_FREEBLOCK_OFFSET: usize = 1;
const PAGE_CELL_COUNT_OFFSET: usize = 3;
const PAGE_CELL_CONTENT_OFFSET: usize = 5;
const PAGE_FRAGMENTED_BYTES_COUNT_OFFSET: usize = 7;



pub enum Page {
    TableLeaf(TableLeafPage),
}

impl Page {

    pub fn parse(buffer: &[u8], page_num: usize) -> anyhow::Result<Self> {
        // https://www.sqlite.org/fileformat.html#b_tree_pages
        // The 100-byte database file header (found on page 1 only)
        // The 8 or 12 byte b-tree page header
        // The cell pointer array
        // Unallocated space
        // The cell content area
        // The reserved region
        let ptr_offset = if page_num == 1 { HEADER_SIZE as u16 } else { 0 };
        match buffer[ptr_offset as usize] {
            TABLE_LEAF_PAGE_ID => {
                let page = TableLeafPage::parse(buffer, ptr_offset)?;
                Ok(Self::TableLeaf(page))
            }
            _ => anyhow::bail!("Unknown page type in page parse: {}", buffer[100]),
        }
    }
}

pub struct TableLeafPage {
    pub header: PageHeader,
    pub cell_pointers: Vec<u16>,
    pub cells: Vec<TableLeafCell>,
}
impl TableLeafPage {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        let header = PageHeader::parse(buffer, ptr_offset)?;
        let content_buffer = &buffer[ptr_offset as usize + PAGE_LEAF_HEADER_SIZE..];
        let cell_pointers =
            parse_cell_pointers(content_buffer, header.cell_count as usize, ptr_offset);
        
        let mut cells = Vec::new();
        for &ptr in &cell_pointers {
            let cell = TableLeafCell::parse(&content_buffer[ptr as usize - ptr_offset as usize - PAGE_LEAF_HEADER_SIZE..])?;
            cells.push(cell);
        }
        
        Ok(TableLeafPage {
            header,
            cell_pointers,
            cells,
        })
    }
}

#[derive(Debug)]
pub struct PageHeader {
    page_type: PageType,
    first_freeblock: u16,
    cell_count: u16,
    cell_content_offset: u32,
    fragmented_bytes_count: u8,
}
impl PageHeader {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        let page_type = match buffer[ptr_offset as usize] {
            TABLE_LEAF_PAGE_ID => PageType::TableLeaf,
            _ => anyhow::bail!("Unknown page type in PageHeader parse: {}", buffer[0]),
        };
        let first_freeblock = read_be_word_at(buffer, PAGE_FIRST_FREEBLOCK_OFFSET);
        let cell_count = read_be_word_at(&buffer[ptr_offset as usize..], PAGE_CELL_COUNT_OFFSET);
        
        let cell_content_offset = match read_be_word_at(&buffer[ptr_offset as usize..], PAGE_CELL_CONTENT_OFFSET) {
            0 => 65_536,
            n => n as u32,
        };
        let fragmented_bytes_count = buffer[ptr_offset as usize + PAGE_FRAGMENTED_BYTES_COUNT_OFFSET];
        Ok(PageHeader {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_offset,
            fragmented_bytes_count,
        })
    }
}

#[derive(Debug)]
pub enum PageType {
    TableLeaf,
}

#[derive(Debug)]
pub struct TableLeafCell {
    pub size: u64,
    pub row_id: u64,
    // pub payload: Vec<u8>,
    pub record: Record
}

impl TableLeafCell {
    // Table B-Tree Leaf Cell (header 0x0d):

    // A varint which is the total number of bytes of payload, including any overflow
    // A varint which is the integer key, a.k.a. "rowid"
    // The initial portion of the payload that does not spill to overflow pages.
    // A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    pub fn parse(buffer: &[u8]) -> anyhow::Result<Self> {
        let (n, size) = read_varint_at(buffer, 0)?;
        let buffer = &buffer[n as usize..];

        let (n, row_id) = read_varint_at(buffer, 0)?;
        let buffer = &buffer[n as usize..]; //  start of payload

        // Make sure we don't read beyond the buffer's length
        let payload_size = std::cmp::min(size as usize, buffer.len());
        let payload = buffer[..payload_size].to_vec();
        
        let record = Record::parse(&payload)?;
        Ok(Self {
            size,
            row_id,
            record
        })
    }
}

fn parse_cell_pointers(buffer: &[u8], cell_count: usize, ptr_offset: u16) -> Vec<u16> {
    let mut pointers = Vec::with_capacity(cell_count);
    for i in 0..cell_count {
        let ptr = read_be_word_at(buffer, i * 2);
        pointers.push(ptr);
    }
    pointers
}