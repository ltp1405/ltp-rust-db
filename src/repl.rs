use std::io;

pub struct REPL {}

impl REPL {
    pub fn read_line(&self) -> Result<String, io::Error> {
        let mut buffer = String::new();
        let stdin = std::io::stdin();
        stdin.read_line(&mut buffer)?;
        let buffer = buffer.trim().to_string();
        Ok(buffer)
    }
}
