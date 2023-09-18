use cursive_table_view::{TableView, TableViewItem};
use my_database::table::{
    record::{Field, Record},
    schema::{DataType, Schema},
    table::Table,
};
const BLOCKSIZE: usize = 512;
const CAPACITY: usize = 512 * 4096;
const MEMORY_CAPACITY: usize = 4096 * 32;

use cursive::{
    view::{Nameable, Resizable},
    views::{Button, Dialog, DummyView, LinearLayout, SelectView},
};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
struct ColumnBase(usize);

impl TableViewItem<ColumnBase> for Record {
    fn to_column(&self, column: ColumnBase) -> String {
        self.data[column.0].clone().to_string()
    }
    fn cmp(&self, other: &Self, column: ColumnBase) -> std::cmp::Ordering
    where
        Self: Sized,
    {
        return std::cmp::Ordering::Less;
    }
}

fn make_table(schema: &Schema) -> TableView<Record, ColumnBase> {
    let mut table = TableView::<Record, ColumnBase>::new();
    for (i, field) in schema.schema.iter().enumerate() {
        table.insert_column(
            i,
            ColumnBase(i),
            format!("{} ({:?})", field.0, field.1),
            |c| c,
        );
    }
    table
}

struct App {
    file_system: file_system::FileSystem<'static, BLOCKSIZE, CAPACITY, MEMORY_CAPACITY>,
}

impl App {
    fn init(memory: &'static [u8; MEMORY_CAPACITY], disk: &disk::Disk<BLOCKSIZE, CAPACITY>) -> Self {
        let buffer_manager = buffer_manager::BufferManager::init(memory, &disk);
        let disk_manager = disk_manager::DiskManager::init(&disk);
        App {
            file_system: file_system::FileSystem::open(&buffer_manager, &disk_manager).unwrap(),
        }
    }
}

fn main() {
    let mut siv = cursive::default();
    siv.add_global_callback('q', |s| s.quit());

    let schema = Schema {
        schema: vec![
            (String::from("firstname"), DataType::Char(20)),
            (String::from("lastname"), DataType::Char(20)),
            (String::from("male"), DataType::Bool),
            (String::from("age"), DataType::UInt),
            (String::from("details"), DataType::VarChar(255)),
        ],
    };

    let record = Record {
        data: vec![
            Field::Char(Some(b"Firstname".to_vec())),
            Field::Char(Some(b"Lastname".to_vec())),
            Field::Bool(Some(true)),
            Field::UInt(Some(21)),
            Field::VarChar(Some(b"This is some one.".to_vec())),
        ],
    };

    let mut table = make_table(&schema);

    for _ in 0..10 {
        table.insert_item(record.clone());
    }

    let select = SelectView::<String>::new()
        .item("table", "table 1".to_string())
        .item("table", "table 2".to_string())
        .with_name("Choose a table")
        .fixed_size((20, 10));
    let buttons = LinearLayout::vertical()
        .child(Button::new("Add", on_add_table))
        .child(DummyView)
        .child(Button::new("Delete", |s| {
            s.call_on_name("Choose a table", |view: &mut SelectView<String>| {
                view.selected_id().map(|id| view.remove_item(id));
            });
        }));
    siv.add_layer(Dialog::around(
        LinearLayout::horizontal().child(select).child(buttons),
    ));
    siv.run();
}

fn on_add_table(s: &mut cursive::Cursive) {
    s.call_on_name("Choose a table", |view: &mut SelectView<String>| {
        view.add_item_str("table 3");
    });
}
