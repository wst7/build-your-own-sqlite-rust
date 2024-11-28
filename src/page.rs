use anyhow::Ok;

use crate::{db::HEADER_SIZE, record::Record, utils::{read_be_word_at, read_varint}};


pub const TABLE_LEAF_PAGE_ID: u8 = 0x0d;

const PAGE_LEAF_HEADER_SIZE: usize = 8;
const PAGE_FIRST_FREEBLOCK_OFFSET: usize = 1;
const PAGE_CELL_COUNT_OFFSET: usize = 3;
const PAGE_CELL_CONTENT_OFFSET: usize = 5;
const PAGE_FRAGMENTED_BYTES_COUNT_OFFSET: usize = 7;


#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct TableLeafPage {
    pub header: PageHeader,
    pub cell_pointers: Vec<u16>,
    pub cells: Vec<TableLeafCell>,
}
impl TableLeafPage {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> { // all buffer starts db header
        let header = PageHeader::parse(buffer, ptr_offset)?;

        // 计算单元格指针区域的起始位置（紧跟在页面头部之后）
        let cell_pointer_area_start = ptr_offset as usize + PAGE_LEAF_HEADER_SIZE;
        
        // 解析单元格指针数组
        let cell_pointers = parse_cell_pointers(
            &buffer[cell_pointer_area_start..], 
            header.cell_count as usize,
            ptr_offset
        );
        
        // 解析每个单元格
        let cells = cell_pointers.iter()
        .map(|ptr| TableLeafCell::parse(&buffer[*ptr as usize..]))
        .collect::<anyhow::Result<Vec<TableLeafCell>>>()?;
        

        Ok(TableLeafPage {
            header,
            cell_pointers,
            cells,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PageHeader {
    page_type: PageType,
    first_freeblock: u16,
    cell_count: u16,
    cell_content_offset: u32,
    fragmented_bytes_count: u8,
}
impl PageHeader {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        // 验证页面类型
        let page_type = match buffer[ptr_offset as usize] {
            TABLE_LEAF_PAGE_ID => PageType::TableLeaf,
            other => anyhow::bail!("Unsupported page type: {}", other),
        };

        // 读取页面头部的各个字段
        let first_freeblock = read_be_word_at(
            buffer,
            ptr_offset as usize + PAGE_FIRST_FREEBLOCK_OFFSET
        );
        
        let cell_count = read_be_word_at(
            buffer,
            ptr_offset as usize + PAGE_CELL_COUNT_OFFSET
        );
        
        let cell_content_offset = read_be_word_at(
            buffer,
            ptr_offset as usize + PAGE_CELL_CONTENT_OFFSET
        ) as u32;  // 转换为 u32
        
        let fragmented_bytes_count = buffer[
            ptr_offset as usize + PAGE_FRAGMENTED_BYTES_COUNT_OFFSET
        ];

        Ok(PageHeader {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_offset,
            fragmented_bytes_count,
        })
    }
}

#[derive(Debug, Clone)]
pub enum PageType {
    TableLeaf,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub size: u64,
    pub row_id: u64,
    pub record: Record
}

impl TableLeafCell {
    // Table B-Tree Leaf Cell (header 0x0d):

    // A varint which is the total number of bytes of payload, including any overflow
    // A varint which is the integer key, a.k.a. "rowid"
    // The initial portion of the payload that does not spill to overflow pages.
    // A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    pub fn parse(cell_buffer: &[u8]) -> anyhow::Result<Self> {
        // 有问题
        let (n, payload_size) = read_varint(cell_buffer)?;
        let buffer = &cell_buffer[n as usize..];

        let (n, row_id) = read_varint(buffer)?;
        let buffer = &buffer[n as usize..]; //  start of payload
        

        let payload = buffer[..payload_size as usize].to_vec();
        let record = Record::parse(&payload)?;

        Ok(Self {
            size: payload_size as u64,
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