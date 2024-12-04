pub fn read_be_word_at(buf: &[u8], offset: usize) -> u16 {
  u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

// TODO: WHY NOT WORK
// pub fn read_varint(buffer: &[u8]) -> anyhow::Result<(usize, u64)> {
//     // println!("read varint buffer: {:?}", buffer);
//     let mut result = 0u64;
//     let mut bytes_read = 0;
//     let mut offset = 0;
//     loop {
//         let byte = buffer[offset];
//         offset += 1;
//         bytes_read += 1;
//         result <<= 7 * (bytes_read - 1);
//         result |= (byte & 0x7f) as u64;
//         println!("offset: {}, result: {}", offset, result);
//         if byte & 0x80 == 0 {
//             break;
//         }
//     }
//     Ok((bytes_read, result))
// }

// TODO: optimize
pub fn read_varint(bytes: &[u8]) -> anyhow::Result<(usize, u64)> {

    let mut trimmed_bytes: Vec<u8> = Vec::new();
    let mut continue_bit = true;
    for (i, byte) in bytes.iter().enumerate() {
        if !continue_bit {
            break;
        }
        continue_bit = (byte & 0b1000_0000) == 0b1000_0000;

        if i == 8 {
            trimmed_bytes.push(*byte);
            break;
        }

        let trimmed_byte = byte & 0b0111_1111;
        trimmed_bytes.push(trimmed_byte);
    }

    let mut res = 0_u64;
    for (i, byte) in trimmed_bytes.iter().enumerate() {
        if i == 8 {
            res <<= 8;
            res |= *byte as u64;
            break;
        }

        res <<= 7;
        res |= *byte as u64;
    }

    anyhow::Ok((trimmed_bytes.len(), res))
}
