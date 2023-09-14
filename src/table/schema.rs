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
    pub schema: Vec<(String, DataType)>,
}

impl Schema {
    pub fn get_field_type(&self, index: usize) -> &DataType {
        &self.schema[index].1
    }

    pub fn serialize(&self) -> Vec<Vec<u8>> {
        let mut v = Vec::new();
        for field in &self.schema {
            v.push(Vec::new());
            match field.1 {
                DataType::Char(n) => {
                    v.last_mut().unwrap().push(b'c');
                    v.last_mut().unwrap().extend_from_slice(&n.to_be_bytes());
                }
                DataType::Bool => v.last_mut().unwrap().push(b'b'),
                DataType::UInt => v.last_mut().unwrap().push(b'u'),
                DataType::Int => v.last_mut().unwrap().push(b'i'),
                DataType::Float => v.last_mut().unwrap().push(b'f'),
                DataType::VarChar(n) => {
                    v.last_mut().unwrap().push(b'v');
                    v.last_mut().unwrap().extend_from_slice(&n.to_be_bytes());
                }
                _ => todo!(),
            }
        }
        v.into_iter()
            .enumerate()
            .map(|(i, mut item)| {
                item.extend(self.schema[i].0.as_bytes());
                item
            })
            .collect()
    }

    pub fn deserialize(fields: Vec<Vec<u8>>) -> Option<Self> {
        let mut fieldtypes: Vec<DataType> = Vec::new();
        let mut fieldnames: Vec<String> = Vec::new();
        for field in fields {
            match field[0] {
                b'c' => {
                    let n = u32::from_be_bytes(field[1..1 + size_of::<u32>()].try_into().unwrap());
                    fieldtypes.push(DataType::Char(n));
                    fieldnames.push(String::from_utf8(field[5..].to_vec()).unwrap());
                }
                b'b' => {
                    fieldtypes.push(DataType::Bool);
                    fieldnames.push(String::from_utf8(field[1..].to_vec()).unwrap());
                }
                b'u' => {
                    fieldtypes.push(DataType::UInt);
                    fieldnames.push(String::from_utf8(field[1..].to_vec()).unwrap());
                }
                b'i' => {
                    fieldtypes.push(DataType::Int);
                    fieldnames.push(String::from_utf8(field[1..].to_vec()).unwrap());
                }
                b'f' => {
                    fieldtypes.push(DataType::Float);
                    fieldnames.push(String::from_utf8(field[1..].to_vec()).unwrap());
                }
                b'v' => {
                    let n = u32::from_be_bytes(field[1..1 + size_of::<u32>()].try_into().unwrap());
                    fieldtypes.push(DataType::VarChar(n));
                    fieldnames.push(String::from_utf8(field[5..].to_vec()).unwrap());
                }
                _ => return None,
            }
        }
        Some(Self {
            schema: fieldnames.into_iter().zip(fieldtypes.into_iter()).collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = Schema {
            schema: vec![
                (String::from("sldgnnq"), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::UInt),
                (String::from("f;ljkdjsbdg"), DataType::Int),
                (String::from("sldklaldng"), DataType::Float),
                (String::from("slnhshwker;hypwi"), DataType::VarChar(20)),
            ],
        };
        let buf = schema.serialize();
        let schema2 = Schema::deserialize(buf);
        assert_eq!(schema.schema, schema2.unwrap().schema);
    }

    #[test]
    fn more_schema() {
        let schema = Schema {
            schema: vec![
                (String::new(), DataType::Char(10)),
                (String::new(), DataType::Bool),
                (String::new(), DataType::UInt),
                (String::new(), DataType::Int),
                (String::new(), DataType::Float),
                (String::new(), DataType::VarChar(255)),
            ],
        };
        let buf = schema.serialize();
        let schema2 = Schema::deserialize(buf);
        assert_eq!(schema.schema, schema2.unwrap().schema);
    }
}
