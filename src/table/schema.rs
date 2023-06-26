enum Field {
    Char(u8, usize),
    Bool(bool),
    UInt(u32),
    Int(i32),
    Float(f32),
    VarChar(usize),
}

enum DataType {
    Char(usize),
    Bool,
    UInt,
    Int,
    Float,
    VarChar(usize),
}

struct Schema {
    schema: Vec<Field>,
}
