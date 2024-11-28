pub fn read_be_word_at(buf: &[u8], offset: usize) -> u16 {
  u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

pub fn read_varint(buffer: &[u8]) -> anyhow::Result<(usize, u64)> {
    // println!("read varint buffer: {:?}", buffer);
    let mut result = 0u64;
    let mut bytes_read = 0;
    let mut offset = 0;
    loop {
        let byte = buffer[offset];
        offset += 1;
        bytes_read += 1;
        result <<= 7 * (bytes_read - 1);
        result |= (byte & 0x7f) as u64;
        // println!("offset: {}, result: {}", offset, result);
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok((bytes_read, result))
}