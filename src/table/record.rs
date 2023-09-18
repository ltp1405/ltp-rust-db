use std::{fmt::Display, mem::size_of};

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

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Char(data) => match data {
                Some(value) => write!(f, "{}", String::from_utf8(value.to_vec()).unwrap()),
                None => write!(f, "None"),
            },
            Self::Bool(data) => match data {
                Some(value) => write!(f, "{}", value),
                None => write!(f, "None"),
            },
            Self::UInt(data) => match data {
                Some(value) => write!(f, "{}", value),
                None => write!(f, "None"),
            },
            Self::Int(data) => match data {
                Some(value) => write!(f, "{}", value),
                None => write!(f, "None"),
            },
            Self::Float(data) => match data {
                Some(value) => write!(f, "{}", value),
                None => write!(f, "None"),
            },
            Self::VarChar(data) => match data {
                Some(value) => write!(f, "{}", String::from_utf8(value.to_vec()).unwrap()),
                None => write!(f, "None"),
            },
            _ => panic!(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Record {
    pub data: Vec<Field>,
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let last_idx = self.data.len() - 1;
        write!(f, "{}", self.data[last_idx])?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct InvalidSchema;

impl Record {
    pub fn to_bytes(self, schema: &Schema) -> Result<Vec<u8>, InvalidSchema> {
        let mut buf: Vec<u8> = Vec::new();
        for (i, field) in self.data.into_iter().enumerate() {
            let field_type = schema.get_field_type(i);
            match field {
                Field::Char(s) => {
                    if let Some(mut s) = s {
                        let char_len = match field_type {
                            DataType::Char(n) => *n as usize,
                            _ => return Err(InvalidSchema),
                        };
                        s.resize(char_len, b'\0');
                        buf.extend_from_slice(&s[..char_len]);
                    }
                }
                Field::CharUtf8(s) => {
                    if let Some(s) = s {
                        let char_len = match field_type {
                            DataType::CharUtf8(n) => *n as usize * size_of::<char>(),
                            _ => return Err(InvalidSchema),
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
                            _ => return Err(InvalidSchema),
                        };
                        buf.extend_from_slice(&s.as_bytes()[..char_len]);
                    }
                }
            }
            buf.push(b'|');
        }
        buf.pop();
        buf.push(b'\n');
        Ok(buf)
    }

    pub fn from_bytes(buf: Vec<u8>, schema: &Schema) -> Result<Self, InvalidSchema> {
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
                    let field = field[..*n as usize].split(|&c| c == b'\0').next();
                    let field = match field {
                        Some(field) => field,
                        None => return Err(InvalidSchema),
                    };
                    fields.push(Field::Char(Some(field.to_vec())));
                }
                DataType::CharUtf8(n) => {
                    if field.len() == 0 {
                        fields.push(Field::CharUtf8(None));
                        continue;
                    }
                    let field = field[..*n as usize].split(|&c| c == b'\0').next();
                    let field = match field {
                        Some(field) => field,
                        None => return Err(InvalidSchema),
                    };
                    fields.push(Field::CharUtf8(Some(
                        String::from_utf8(field.to_vec()).unwrap(),
                    )));
                }
                DataType::Bool => {
                    if field.len() == 0 {
                        fields.push(Field::Bool(None));
                        continue;
                    }
                    if field.len() > 1 {
                        return Err(InvalidSchema);
                    }
                    if field[0] != 0 && field[0] != 1 {
                        return Err(InvalidSchema);
                    }
                    fields.push(Field::Bool(Some(field[0] != 0)));
                }
                DataType::UInt => {
                    if field.len() == 0 {
                        fields.push(Field::UInt(None));
                        continue;
                    }
                    if field.len() > size_of::<u32>() {
                        return Err(InvalidSchema);
                    }
                    fields.push(Field::UInt(Some(u32::from_be_bytes(
                        field[..].try_into().unwrap(),
                    ))));
                }
                DataType::Int => {
                    if field.len() == 0 {
                        fields.push(Field::Int(None));
                        continue;
                    }
                    if field.len() > size_of::<i32>() {
                        return Err(InvalidSchema);
                    }
                    fields.push(Field::Int(Some(i32::from_be_bytes(
                        field[..].try_into().unwrap(),
                    ))));
                }
                DataType::Float => {
                    if field.len() == 0 {
                        fields.push(Field::Float(None));
                        continue;
                    }
                    if field.len() > size_of::<f32>() {
                        return Err(InvalidSchema);
                    }
                    fields.push(Field::Float(Some(f32::from_be_bytes(
                        field[..].try_into().unwrap(),
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
        Ok(Record { data: fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record() {
        let schema = Schema {
            schema: vec![
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::UInt),
                (String::new(), DataType::VarChar(255)),
            ],
        };
        let record = Record {
            data: vec![
                Field::Char(Some(b"Hello".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes(&schema).unwrap();
        let record2 = Record::from_bytes(bytes, &schema).unwrap();
        assert_eq!(record, record2);

        let record = Record {
            data: vec![
                Field::Char(Some(b"Hello, World".to_vec())),
                Field::Bool(Some(true)),
                Field::UInt(Some(42)),
                Field::VarChar(Some(b"World".to_vec())),
            ],
        };
        let bytes = record.clone().to_bytes(&schema).unwrap();
        let record2 = Record::from_bytes(bytes, &schema).unwrap();
        match record2.data[0] {
            Field::Char(Some(ref s)) => assert_eq!(s.len(), 10),
            _ => panic!("Invalid field type"),
        }
    }
}
