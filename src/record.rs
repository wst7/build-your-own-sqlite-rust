use core::str;
use std::fmt::format;

use crate::utils::read_varint;

#[derive(Debug, Clone)]
pub enum RecordFieldType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    Float,
    Zero,
    One,
    String,
    Blob,
    Variable,
}

#[derive(Debug, Clone)]
pub struct RecordField {
    // pub offset: usize,
    pub field_size: usize,
    pub field_type: RecordFieldType,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub fields: Vec<RecordField>,
}

impl RecordHeader {
    pub fn parse(payload: &[u8]) -> anyhow::Result<(Self, usize)> {
        let (varint_size, header_length) = read_varint(payload)?;
        
        let mut buffer = &payload[varint_size..header_length as usize]; // header_length
        let mut current_offset = varint_size;
        let mut fields = Vec::new();
        
        while !buffer.is_empty() && current_offset < header_length as usize {
            let (byte_read, field_type) = read_varint(buffer)?;
            let (field_type, field_size) = match field_type {
                0 => (RecordFieldType::Null, 0),
                1 => (RecordFieldType::I8, 1),
                2 => (RecordFieldType::I16, 2),
                3 => (RecordFieldType::I24, 3),
                4 => (RecordFieldType::I32, 4),
                5 => (RecordFieldType::I48, 6),
                6 => (RecordFieldType::I64, 8),
                7 => (RecordFieldType::Float, 8),
                8 => (RecordFieldType::Zero, 0),
                9 => (RecordFieldType::One, 0),
                n if n > 12 && n % 2 == 0 => {
                    let size = ((n - 12) / 2) as usize;
                    (RecordFieldType::Blob, size)
                }
                n if n >= 13 && n % 2 == 1 => {
                    let size = ((n - 13) / 2) as usize;
                    (RecordFieldType::String, size)
                }
                n => todo!("unsupported field type: {}", n),
            };
            
            fields.push(RecordField {
                field_size,
                field_type,
            });
            buffer = &buffer[byte_read..];
            current_offset += byte_read;
        }
        
        Ok((RecordHeader { fields }, current_offset as usize ))
    }
}

#[derive(Debug, Clone)]
pub struct RecordBody {
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub header: RecordHeader,
    pub body: Vec<RecordBody>,
}

impl Record {
    pub fn parse(payload: &[u8], row_id: u64) -> anyhow::Result<Self> {
        let (header, header_length) = RecordHeader::parse(payload)?;
        let mut body = Vec::new();
        let mut offset = header_length;
        for field in header.fields.iter() {
            let value = match field.field_type {
                RecordFieldType::Null => Value::I64(row_id as i64),
                RecordFieldType::I8 => {
                    let val = read_i8_at(payload, offset);
                    Value::I64(val as i64)
                },
                RecordFieldType::I16 => {
                    let val = read_i16_at(payload, offset);
                    Value::I64(val as i64)
                },
                RecordFieldType::I24 => {
                    let val = read_i24_at(payload, offset);
                    Value::I64(val as i64)
                },
                RecordFieldType::I32 => {
                    let val = read_i32_at(payload, offset);
                    Value::I64(val as i64)
                },
                RecordFieldType::I48 => {
                    let val = read_i48_at(payload, offset);
                    Value::I64(val)
                },
                RecordFieldType::I64 => {
                    let val = read_i64_at(payload, offset);
                    Value::I64(val)
                },
                RecordFieldType::Float => Value::Float(read_f64_at(payload, offset)),
                RecordFieldType::Zero => Value::I64(0),
                RecordFieldType::One => Value::I64(1),
                RecordFieldType::String => {
                    let value = String::from_utf8(payload[offset..offset + field.field_size].to_vec())?;
                    Value::String(value)
                }
                RecordFieldType::Blob => {
                    let value = payload[offset..offset + field.field_size].to_vec();
                    Value::Blob(value)
                }
                RecordFieldType::Variable => Value::Null,
            };
            body.push(RecordBody { value });
            offset += field.field_size;
        }
        // println!("body: {:#?}", body);
        Ok(Record { header, body })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    I64(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Self::Null => format!("NULL"),
            Self::I64(n) => format!("{n}"),
            Self::Float(n) => format!("{n}"),
            Self::String(s) => s.clone(),
            Self::Blob(v) => std::str::from_utf8(v).unwrap().to_string(),
        }
    }
}

// impl PartialOrd for Value {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         match (self, other) {
//             (Self::I64(a), Self::I64(b)) => a.partial_cmp(b),
//             (Self::String(a), Self::String(b)) => a.partial_cmp(b),
//             _ => None,
//         }
//     }
// }
// impl PartialEq for Value {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (Self::I64(a), Self::I64(b)) => a == b,
//             (Self::String(a), Self::String(b)) => a == b,
//             _ => false,
//         }
//     }
    
// }
pub fn read_i8_at(input: &[u8], offset: usize) -> i8 {
    input[offset] as i8
}

pub fn read_i16_at(input: &[u8], offset: usize) -> i16 {
    i16::from_be_bytes(input[offset..offset + 2].try_into().unwrap())
}

pub fn read_i24_at(input: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes([0, input[offset], input[offset + 1], input[offset + 2]])
}

pub fn read_i32_at(input: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(input[offset..offset + 4].try_into().unwrap())
}

pub fn read_i48_at(input: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(input[offset..offset + 6].try_into().unwrap())
}

pub fn read_i64_at(input: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(input[offset..offset + 8].try_into().unwrap())
}

pub fn read_f64_at(input: &[u8], offset: usize) -> f64 {
    f64::from_be_bytes(input[offset..offset + 8].try_into().unwrap())
}
