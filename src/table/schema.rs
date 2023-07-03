use std::mem::size_of;

#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum DataType {
    Char(u32),
    CharUtf8(u32),
    Bool,
    UInt,
    Int,
    Float,
    VarChar(u32),
    VarCharUtf8(u32),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    pub schema: Vec<DataType>,
}

impl Schema {
    pub fn get_field_type(&self, index: usize) -> &DataType {
        &self.schema[index]
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for field in &self.schema {
            match field {
                DataType::Char(n) => {
                    buf.push(b'c');
                    buf.extend_from_slice(&n.to_be_bytes());
                }
                DataType::Bool => buf.push(b'b'),
                DataType::UInt => buf.push(b'u'),
                DataType::Int => buf.push(b'i'),
                DataType::Float => buf.push(b'f'),
                DataType::VarChar(n) => {
                    buf.push(b'v');
                    buf.extend_from_slice(&n.to_be_bytes());
                }
                _ => todo!(),
            }
            buf.push(b'|');
        }
        buf.pop();
        buf.push(b'\n');
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        let mut schema: Vec<DataType> = Vec::new();
        if buf.len() == 0 {
            return Self { schema };
        } else if buf.last().unwrap() != &b'\n' {
            panic!("Invalid schema, no newline");
        }
        let buf = &buf[..buf.len() - 1];
        let fields = buf.split(|&c| c == b'|');
        for field in fields {
            match field[0] {
                b'c' => {
                    let n = u32::from_be_bytes(field[1..1 + size_of::<u32>()].try_into().unwrap());
                    schema.push(DataType::Char(n));
                }
                b'b' => schema.push(DataType::Bool),
                b'u' => schema.push(DataType::UInt),
                b'i' => schema.push(DataType::Int),
                b'f' => schema.push(DataType::Float),
                b'v' => {
                    let n = u32::from_be_bytes(field[1..1 + size_of::<u32>()].try_into().unwrap());
                    schema.push(DataType::VarChar(n));
                }
                _ => panic!("Invalid schema"),
            }
        }
        Self { schema }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::Int,
                DataType::Float,
                DataType::VarChar(20),
            ],
        };
        let buf = schema.to_bytes();
        let schema2 = Schema::from_bytes(&buf);
        assert_eq!(schema.schema, schema2.schema);
    }

    #[test]
    fn more_schema() {
        let schema = Schema {
            schema: vec![
                DataType::Char(10),
                DataType::Bool,
                DataType::UInt,
                DataType::Int,
                DataType::Float,
                DataType::VarChar(255),
            ],
        };
        let buf = schema.to_bytes();
        let schema2 = Schema::from_bytes(&buf);
        assert_eq!(schema.schema, schema2.schema);
    }
}
