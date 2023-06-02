use my_database::{repl::REPL, Row, Table};

fn main() {
    let mut table = Table::init();
    table.insert_row(Row {
        id: 0,
        username: ['a';32],
        email: ['b';255],
    });
    table.select_row();
    // let repl = REPL {};
    // loop {
    //     let input = repl.read_line().unwrap();
    //     if input.starts_with('.') {
    //         handle_meta_command(input)
    //     } else {
    //         handle_statement(input);
    //     }
    // }
}

fn handle_statement(input: String) {}
