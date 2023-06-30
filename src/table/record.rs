use std::mem::size_of;

use super::schema::{DataType, Schema};

#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum Field {
    Char(Option<Vec<u8>>),
    CharUtf8(Option<String>),
    Bool(Option<bool>),
    UInt(Option<u32>),
    Int(Option<i32>),
    Float(Option<f32>),
    VarChar(Option<Vec<u8>>),
    VarCharUtf8(Option<String>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Record {
    pub schema: Schema,
    pub data: Vec<Field>,
}

impl Record {
    pub fn to_bytes(self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        for (i, field) in self.data.into_iter().enumerate() {
            let field_type = self.schema.get_field_type(i);
            match field {
                Field::Char(s) => {
                    if let Some(mut s) = s {
                        let char_len = match field_type {
                            DataType::Char(n) => *n as usize,
                            _ => panic!("Invalid field type"),
                        };
                        s.resize(char_len, b'\0');
                        buf.extend_from_slice(&s[..char_len]);
                    }
                }
                Field::CharUtf8(s) => {
                    if let Some(s) = s {
                        let char_len = match field_type {
                            DataType::CharUtf8(n) => *n as usize * size_of::<char>(),
                            _ => panic!("Invalid field type"),
                       };
                        buf.extend_from_slice(&s.as_bytes()[..char_len]);
                    }
                }
                Field::Bool(s) => {
                    if let Some(s) = s {
                        buf.push(if s { 1 } else { 0 });
                    }
                }
                Field::UInt(s) => {
                    if let Some(s) = s {
                        buf.extend_from_slice(&s.to_be_bytes());
                    }
                }
                Field::Int(s) => {
                    if let Some(s) = s {
                        buf.extend_from_slice(&s.to_be_bytes());
                    }
                }
                Field::Float(s) => {
                    if let Some(s) = s {
                        buf.extend_from_slice(&s.to_be_bytes());
                    }
                }
                Field::VarChar(s) => {
                    if let Some(s) = s {
                        buf.extend_from_slice(&s);
                    }
                }
                Field::VarCharUtf8(s) => {
                    if let Some(s) = s {
                        let char_len = match field_type {
                            DataType::VarCharUtf8(n) => *n as usize * size_of::<char>(),
                            _ => panic!("Invalid field type"),
                        };
                        buf.extend_from_slice(&s.as_bytes()[..char_len]);
                    }
                }
            }
            buf.push(b'|');
        }
        buf.pop();
        buf.push(b'\n');
        buf
    }

    pub fn from_bytes(schema: Schema, buf: Vec<u8>) -> Self {
        let mut fields = Vec::new();
        let raw_fields = buf.split(|&c| c == b'|');
        for (i, field) in raw_fields.into_iter().enumerate() {
            let field_type = schema.get_field_type(i);
            match field_type {
                DataType::Char(n) => {
                    if field.len() == 0 {
                        fields.push(Field::Char(None));
                        continue;
                    }
                    let field = field[..*n as usize].split(|&c| c == b'\0').next().unwrap();
                    fields.push(Field::Char(Some(field.to_vec())));
                }
                DataType::CharUtf8(n) => {
                    if field.len() == 0 {
                        fields.push(Field::CharUtf8(None));
                        continue;
                    }
                    fields.push(Field::CharUtf8(Some(
                        String::from_utf8(field[..*n as usize].to_vec()).unwrap(),
                    )));
                }
                DataType::Bool => {
                    if field.len() == 0 {
                        fields.push(Field::Bool(None));
                        continue;
                    }
                    fields.push(Field::Bool(Some(field[0] != 0)));
                }
                DataType::UInt => {
                    if field.len() == 0 {
                        fields.push(Field::UInt(None));
                        continue;
                    }
                    fields.push(Field::UInt(Some(u32::from_be_bytes(
                        field[..size_of::<u32>()].try_into().unwrap(),
                    ))));
                }
                DataType::Int => {
                    if field.len() == 0 {
                        fields.push(Field::Int(None));
                        continue;
                    }
                    fields.push(Field::Int(Some(i32::from_be_bytes(
                        field[..size_of::<i32>()].try_into().unwrap(),
                    ))));
                }
                DataType::Float => {
                    if field.len() == 0 {
                        fields.push(Field::Float(None));
                        continue;
                    }
                    fields.push(Field::Float(Some(f32::from_be_bytes(
                        field[..size_of::<f32>()].try_into().unwrap(),
                    ))));
                }
                DataType::VarChar(_) => {
                    if field.len() == 0 {
                        fields.push(Field::VarChar(None));
                        continue;
                    }
                    let field = field.split(|&c| c == b'\0' || c == b'\n').next().unwrap();
                    fields.push(Field::VarChar(Some(field.to_vec())));
                }
                DataType::VarCharUtf8(n) => {
                    if field.len() == 0 {
                        fields.push(Field::VarCharUtf8(None));
                        continue;
                    }
                    fields.push(Field::VarCharUtf8(Some(
                        String::from_utf8(field[..*n as usize].to_vec()).unwrap(),
                    )));
                }
            }
        }
        Record {
            schema,
            data: fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record() {
        let schema = Schema {
            schema: vec![DataType::Char(10), DataType::Bool, DataType::UInt, DataType::VarChar(255)],
        };
        let record = Record {
            schema: schema.clone(),
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes();
        let record2 = Record::from_bytes(schema.clone(), bytes);
        assert_eq!(record, record2);

        let record = Record {
            schema: schema.clone(),
            data: vec![
                Field::Char(Some(b"Hello, World".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes();
        let record2 = Record::from_bytes(schema, bytes);
        match record2.data[0] {
            Field::Char(Some(ref s)) => assert_eq!(s.len(), 10),
            _ => panic!("Invalid field type"),
        }
    }
}
