pub fn read_be_word_at(buf: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

pub fn read_varint(buffer: &[u8]) -> anyhow::Result<(usize, u64)> {
    let mut result = 0u64;
    let mut n = 0;
    loop {
        let byte = buffer[n];
        // b & 0x7F 获取下7bits有效数据
        result = (result << 7) | ((byte & 0x7F) as u64);
        n += 1;
        if byte & 0x80 == 0 { // 取高位1继续，0终止
            break;
        }
        if n >= 9 {
            anyhow::bail!("varint too long"); 
        } 
    }
    Ok((n, result))
}

