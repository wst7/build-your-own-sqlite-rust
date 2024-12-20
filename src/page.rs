use anyhow::Ok;

use crate::{
    db::HEADER_SIZE,
    record::Record,
    utils::{read_be_word_at, read_varint},
};

pub const TABLE_LEAF_PAGE_ID: u8 = 0x0d;
pub const TABLE_INTERIOR_PAGE_ID: u8 = 0x05;
pub const INDEX_LEAF_PAGE_ID: u8 = 0x0a;
pub const INDEX_INTERIOR_PAGE_ID: u8 = 0x02;

const PAGE_LEAF_HEADER_SIZE: usize = 8;
const PAGE_INTERIOR_HEADER_SIZE: usize = 12;

const PAGE_FIRST_FREEBLOCK_OFFSET: usize = 1;
const PAGE_CELL_COUNT_OFFSET: usize = 3;
const PAGE_CELL_CONTENT_OFFSET: usize = 5;
const PAGE_FRAGMENTED_BYTES_COUNT_OFFSET: usize = 7;
const PAGE_RIGHT_MOST_POINTER_OFFSET: usize = 8;


  /**

                    [Interior Table Page (Root)]
                    +---------------------------+
                    | Left Child  | RowID | Right Child |
                    +---------------------------+
                        /                            \
                        /                              \
        [Interior Table Page]                 [Leaf Table Page]
        +---------------------+                +----------------------+
        | Left Child | RowID  |               | RowID | Record (Row) |
        +---------------------+               +----------------------+
            /         \                             /         \        
            /           \                           /           \
    [Leaf Table Page]  [Leaf Table Page]   [RowID=3]   [RowID=5]
    +----------------+  +----------------+
    | RowID | Record |  | RowID | Record |
    +----------------+  +----------------+
    */


#[derive(Debug, Clone)]
pub enum Page {
    TableLeaf(TableLeafPage),
    TableInterior(TableInteriorPage),
    IndexLeaf(IndexLeafPage),
    IndexInterior(IndexInteriorPage),
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
        let page_type = buffer[ptr_offset as usize];
       
        match page_type {
            TABLE_LEAF_PAGE_ID => {
                let page = TableLeafPage::parse(buffer, ptr_offset)?;
                Ok(Self::TableLeaf(page))
            }
            TABLE_INTERIOR_PAGE_ID => {
                let page = TableInteriorPage::parse(buffer, ptr_offset)?;
                Ok(Self::TableInterior(page))
            }
            INDEX_LEAF_PAGE_ID => {
                let page = IndexLeafPage::parse(buffer, ptr_offset)?;
                Ok(Self::IndexLeaf(page))
            }
            INDEX_INTERIOR_PAGE_ID => {
                let page = IndexInteriorPage::parse(buffer, ptr_offset)?;
                Ok(Self::IndexInterior(page))
            }
            _ => {
                anyhow::bail!("Unknown page type in page parse: {}", page_type)
            }
        }
    }

    pub fn get_page_type(&self) -> &PageType {
        match self {
            Page::TableLeaf(page) => page.header.get_page_type(),
            Page::TableInterior(page) => page.header.get_page_type(),
            Page::IndexLeaf(page) => page.header.get_page_type(),
            Page::IndexInterior(page) => page.header.get_page_type(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableLeafPage {
    pub header: PageHeader,
    pub cells: Vec<TableLeafCell>,
}
impl TableLeafPage {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        // all buffer starts db header
        let header = PageHeader::parse(buffer, ptr_offset)?;

        // 计算单元格指针区域的起始位置（紧跟在页面头部之后）
        let cell_pointer_area_start = ptr_offset as usize + PAGE_LEAF_HEADER_SIZE;

        // 解析单元格指针数组
        let cell_pointers = parse_cell_pointers(
            &buffer[cell_pointer_area_start..],
            header.cell_count as usize,
            ptr_offset,
        );
        // 解析每个单元格
        let cells = cell_pointers
            .iter()
            .map(|ptr| TableLeafCell::parse(&buffer[*ptr as usize..]))
            .collect::<anyhow::Result<Vec<TableLeafCell>>>()?;
        Ok(TableLeafPage {
            header,
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
    right_most_point: u32,
}
impl PageHeader {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        // 验证页面类型
        let page_type = match buffer[ptr_offset as usize] {
            TABLE_LEAF_PAGE_ID => PageType::TableLeaf,
            TABLE_INTERIOR_PAGE_ID => PageType::TableInterior,
            INDEX_LEAF_PAGE_ID => PageType::IndexLeaf,
            INDEX_INTERIOR_PAGE_ID => PageType::IndexInterior,
            other => anyhow::bail!("Unsupported page type: {}", other),
        };

        // 读取页面头部的各个字段
        let first_freeblock =
            read_be_word_at(buffer, ptr_offset as usize + PAGE_FIRST_FREEBLOCK_OFFSET);

        let cell_count = read_be_word_at(buffer, ptr_offset as usize + PAGE_CELL_COUNT_OFFSET);

        let cell_content_offset =
            read_be_word_at(buffer, ptr_offset as usize + PAGE_CELL_CONTENT_OFFSET) as u32; // 转换为 u32

        let fragmented_bytes_count =
            buffer[ptr_offset as usize + PAGE_FRAGMENTED_BYTES_COUNT_OFFSET];
        let right_most_point = if page_type == PageType::TableLeaf || page_type == PageType::IndexLeaf {
            0
        } else {
            u32::from_be_bytes(
                buffer[ptr_offset as usize + PAGE_RIGHT_MOST_POINTER_OFFSET
                    ..PAGE_INTERIOR_HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            )
        };

        Ok(PageHeader {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_offset,
            fragmented_bytes_count,
            right_most_point,
        })
    }

    pub fn get_right_most_point(&self) -> u32 {
        self.right_most_point
    }

    pub fn get_cell_count(&self) -> u16 {
        self.cell_count
    }

    pub fn get_page_type(&self) -> &PageType {
        &self.page_type
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PageType {
    TableLeaf,
    TableInterior,
    IndexLeaf,
    IndexInterior,
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub size: u64,
    pub row_id: u64,
    pub record: Record,
}

impl TableLeafCell {
    // Table B-Tree Leaf Cell (header 0x0d):

    // A varint which is the total number of bytes of payload, including any overflow
    // A varint which is the integer key, a.k.a. "rowid"
    // The initial portion of the payload that does not spill to overflow pages.
    // A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    pub fn parse(cell_buffer: &[u8]) -> anyhow::Result<Self> {
        let (n, payload_size) = read_varint(cell_buffer)?;
        let buffer = &cell_buffer[n as usize..];

        let (n, row_id) = read_varint(buffer)?;
        let buffer = &buffer[n as usize..]; //  start of payload

        let payload = buffer[..payload_size as usize].to_vec();
        let record = Record::parse(&payload, row_id)?;
        Ok(Self {
            size: payload_size as u64,
            row_id,
            record,
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

#[derive(Debug, Clone)]
pub struct TableInteriorPage {
    pub header: PageHeader,
    pub cells: Vec<TableInteriorCell>,
}

impl TableInteriorPage {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        let header = PageHeader::parse(buffer, ptr_offset)?;
        // 计算单元格指针区域的起始位置（紧跟在页面头部之后）
        let cell_pointer_area_start = ptr_offset as usize + PAGE_INTERIOR_HEADER_SIZE;

        let cell_pointers = parse_cell_pointers(
            &buffer[cell_pointer_area_start..],
            header.cell_count as usize,
            ptr_offset,
        );

        let cells = cell_pointers
            .iter()
            .map(|ptr| TableInteriorCell::parse(&buffer[*ptr as usize..]))
            .collect::<anyhow::Result<Vec<TableInteriorCell>>>()?;

        Ok(TableInteriorPage {
            header,
            cells,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableInteriorCell {
    pub row_id: u64,
    pub left_child: u32,
}

impl TableInteriorCell {
    pub fn parse(cell_buffer: &[u8]) -> anyhow::Result<Self> {
        let left_child = u32::from_be_bytes(cell_buffer[0..4].try_into().unwrap());
        let buffer = &cell_buffer[4..];
        let (n, row_id) = read_varint(buffer)?;
        Ok(TableInteriorCell { row_id, left_child })
    }
}


#[derive(Debug, Clone)]
pub struct IndexLeafPage {
    pub header: PageHeader,
    pub cells: Vec<IndexLeafCell>,
}

impl IndexLeafPage {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        let header = PageHeader::parse(buffer, ptr_offset)?;
        let cell_pointer_area_start = ptr_offset as usize + PAGE_LEAF_HEADER_SIZE;
        let cell_pointers = parse_cell_pointers(
            &buffer[cell_pointer_area_start..],
            header.cell_count as usize,
            ptr_offset,
        );
        let cells = cell_pointers
            .iter()
            .map(|ptr| IndexLeafCell::parse(&buffer[*ptr as usize..]))
            .collect::<anyhow::Result<Vec<IndexLeafCell>>>()?;
        Ok(IndexLeafPage {
            header,
            cells,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    pub size: usize,
    pub record: Record,
}

impl IndexLeafCell {
    pub fn parse(cell_buffer: &[u8]) -> anyhow::Result<Self> {
        let (n, payload_size) = read_varint(cell_buffer)?;
        let buffer = &cell_buffer[n as usize..];

        let record = Record::parse(buffer, 0)?;
        Ok(Self {
            size: payload_size as usize,
            record,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexInteriorPage {
    pub header: PageHeader,
    pub cells: Vec<IndexInteriorCell>,
}

impl IndexInteriorPage {
    pub fn parse(buffer: &[u8], ptr_offset: u16) -> anyhow::Result<Self> {
        let header = PageHeader::parse(buffer, ptr_offset)?;
        let cell_pointer_area_start = ptr_offset as usize + PAGE_INTERIOR_HEADER_SIZE;
        let cell_pointers = parse_cell_pointers(
            &buffer[cell_pointer_area_start..],
            header.cell_count as usize,
            ptr_offset,
        );
        let cells = cell_pointers
            .iter()
            .map(|ptr| IndexInteriorCell::parse(&buffer[*ptr as usize..]))
            .collect::<anyhow::Result<Vec<IndexInteriorCell>>>()?;

        Ok(IndexInteriorPage {
            header,
            cells,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexInteriorCell {
    pub size: usize,
    pub left_child: u32,
    pub record: Record,
}

impl IndexInteriorCell {
    pub fn parse(buffer: &[u8]) -> anyhow::Result<Self> {
        let left_child = u32::from_be_bytes(buffer[0..4].try_into().unwrap());
        let buffer = &buffer[4..];
        let (n, payload_size) = read_varint(buffer)?;
        let buffer = &buffer[n as usize..];
        let record = Record::parse(buffer, 0)?;
        Ok(Self {
            size: payload_size as usize,
            left_child,
            record,
        })
    }
}