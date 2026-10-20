use std::io::{self, ErrorKind};

pub struct PgoutputReader<'a> {
    data: &'a [u8],
    position: usize,
}

pub fn invalid_data(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

impl<'a> PgoutputReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == self.position
    }

    pub fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .filter(|&end| end <= self.data.len())
            .ok_or_else(|| invalid_data("truncated pgoutput message"))?;
        let value = &self.data[self.position..end];
        self.position = end;
        Ok(value)
    }

    pub fn skip(&mut self, length: usize) -> io::Result<()> {
        self.take(length)?;
        Ok(())
    }

    pub fn peek_u8(&self) -> io::Result<u8> {
        self.data
            .get(self.position)
            .copied()
            .ok_or_else(|| invalid_data("truncated pgoutput message"))
    }

    pub fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    /**
     * Ok(&[0x12, 0x34]) -> [0x12, 0x34] -> 0x1234
     */
    pub fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn i32(&mut self) -> io::Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn cstring(&mut self) -> io::Result<String> {
        let remaining = &self.data[self.position..];
        let length = remaining
            .iter()
            .position(|&byte| byte == 0)
            .ok_or_else(|| invalid_data("unterminated pgoutput string"))?;
        let value = String::from_utf8_lossy(self.take(length)?).into_owned();
        self.skip(1)?;
        Ok(value)
    }
}
