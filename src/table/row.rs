use super::{COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};

#[derive(Debug, PartialEq, Clone)]
#[repr(C)]
pub struct Row {
    pub id: i32,
    pub username: [char; COLUMN_USERNAME_SIZE],
    pub email: [char; COLUMN_EMAIL_SIZE],
}

impl Row {
    pub fn new(id: i32, username_data: &str, email_data: &str) -> Self {
        let mut email = ['\0'; COLUMN_EMAIL_SIZE];
        let mut username = ['\0'; COLUMN_USERNAME_SIZE];
        for (i, c) in username_data.chars().enumerate() {
            username[i] = c;
        }
        for (i, c) in email_data.chars().enumerate() {
            email[i] = c;
        }
        Self {
            id,
            username,
            email,
        }
    }
}
