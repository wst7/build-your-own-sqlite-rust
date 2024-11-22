
pub fn read_be_word_at(buf: &[u8], offset: usize) -> u16 {
  u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap())
}

pub fn read_varint_at(buffer: &[u8], mut offset: usize) -> anyhow::Result<(usize, u64)> {
  let mut value = 0;
  let mut bytes_read = 0;
  for &byte in buffer.iter() {
      // 提取有效位: 按位与操作 (&) 将字节的最高位（MSB，标志位）清零，保留低 7 位有效数据
      let current_value = byte & 0x7F;

      // 累加到最终结果中
      // •	操作含义：
      // •	current_value << offset
      // •	将当前字节的有效位左移 offset 位，确保其位置正确。
      // •	例如，第一字节占用最低 7 位，第二字节占用第 8-14 位，以此类推。
      // •	value |= ...：
      // •	使用按位或操作 (|=) 将左移后的数据合并到最终结果中。
      value |= current_value << offset;
      // 更新位移量
      offset += 7;
      bytes_read += 1;
      // 如果最高位为 0，解析完成
      if byte & 0x80 == 0 {
          return anyhow::Ok((bytes_read, value.into()));
      }
      // Varint 的最大长度为 9 字节，是因为它用于表示 64 位无符号整数（u64）
      if bytes_read > 9 {
          anyhow::bail!("varint too long")
      }
  }
  anyhow::bail!("Buffer too short for Varint")
}
