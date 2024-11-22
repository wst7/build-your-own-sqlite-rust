use core::str;

use crate::utils::read_varint_at;

#[derive(Debug)]
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
    Variable
}

#[derive(Debug)]
pub struct RecordField {
    pub offset: usize,
    pub field_size: usize,
    pub field_type: RecordFieldType,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub fields: Vec<RecordField>,
}

impl RecordHeader {
    pub fn parse(mut buffer: &[u8]) -> anyhow::Result<Self> {
       
        let (n, length) = read_varint_at(buffer, 0)?;
       
        if n as usize > buffer.len() {
            anyhow::bail!("buffer too short for varint");
        }
        buffer = &buffer[n as usize..];

        let mut fields = Vec::new();
        let mut current_offset = length as usize;
        while !buffer.is_empty() {
            let (n, field_type) = read_varint_at(buffer, 0)?;
           buffer = &buffer[n as usize..];

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
                    10 | 11 => (RecordFieldType::Variable, 0),
                    n if n > 12 && n % 2 == 0=>  {
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
                offset: current_offset,
                field_size,
                field_type,
            });
            current_offset += field_size;
        }

        Ok(RecordHeader { fields })
    }
}

#[derive(Debug)]
pub struct RecordBody {
    pub value: Value,
}

#[derive(Debug)]
pub struct Record {
    pub header: RecordHeader,
    pub body: Vec<RecordBody>,
}


impl Record {
    pub fn parse(buffer: &[u8]) -> anyhow::Result<Self> {
        let header = RecordHeader::parse(buffer)?;
        let mut body = Vec::new();
        for field in header.fields.iter() {
            if field.offset + field.field_size > buffer.len() {
                break;  // Stop if we would read past the buffer
            }
            let value = match field.field_type {
                RecordFieldType::Null => Value::Null,
                RecordFieldType::I8 => Value::I8(read_i8_at(buffer, field.offset)),
                RecordFieldType::I16 => Value::I16(read_i16_at(buffer, field.offset)),
                RecordFieldType::I24 => Value::I24(read_i24_at(buffer, field.offset)),
                RecordFieldType::I32 => Value::I32(read_i32_at(buffer, field.offset)),
                RecordFieldType::I48 => Value::I48(read_i48_at(buffer, field.offset)),
                RecordFieldType::I64 => Value::I64(read_i64_at(buffer, field.offset)),
                RecordFieldType::Float => Value::Float(read_f64_at(buffer, field.offset)),
                RecordFieldType::Zero => Value::Zero,
                RecordFieldType::One => Value::One,
                RecordFieldType::String => {
                    if field.offset + field.field_size > buffer.len() {
                        Value::String(String::new())  // Return empty string if buffer is too short
                    } else {
                        let value = str::from_utf8(&buffer[field.offset..field.offset + field.field_size])
                            .unwrap_or("")
                            .to_string();
                        Value::String(value)
                    }

                }
                RecordFieldType::Blob => {
                    if field.offset + field.field_size > buffer.len() {
                        Value::Blob(Vec::new())  // Return empty vector if buffer is too short
                    } else {
                        let value = buffer[field.offset..field.offset + field.field_size].to_vec();
                        Value::Blob(value)
                    }
                }
                RecordFieldType::Variable => Value::Null,
            };
            body.push(RecordBody { value });
        }
        Ok(Record { header, body })
    }
}

#[derive(Debug)]
pub enum Value {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    Float(f64),
    Zero,
    One,
    String(String),
    Blob(Vec<u8>),
}
pub fn read_i8_at(input: &[u8], offset: usize) -> i8 {
    input[offset] as i8
}

pub fn read_i16_at(input: &[u8], offset: usize) -> i16 {
    i16::from_be_bytes(input[offset..offset + 2].try_into().unwrap())
}

pub fn read_i24_at(input: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(input[offset..offset + 3].try_into().unwrap())
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