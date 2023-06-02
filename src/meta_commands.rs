use std::process::{exit, ExitCode};

pub enum MetaCommandError {
    UnrecognizedCommand,
}

pub fn handle_meta_command(input: String) -> Result<(), MetaCommandError> {
    if input == ".exit" {
        exit(0x1);
    } else {
        Err(MetaCommandError::UnrecognizedCommand)
    }
}
