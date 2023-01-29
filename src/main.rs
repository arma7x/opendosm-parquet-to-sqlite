use sqlite;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use std::{fs::File, path::Path};

use std::ffi::CString;
use libc::{c_char, c_int};
use sqlite3_sys;

// https://storage.googleapis.com/dosm-public-pricecatcher/pricecatcher_yyyy-mm.parquet
// date,premise_code,item_code,price
pub struct Price {
    pub date: i64,
    pub premise_code: i64,
    pub item_code: i64,
    pub price: f64,
}

fn push_price(record: Row, memory_db: &sqlite::Connection) {
    let temp = Price {
        date: record.get_timestamp_micros(0).unwrap(),
        premise_code: record.get_int(1).unwrap() as i64,
        item_code: record.get_int(2).unwrap() as i64,
        price: record.get_double(3).unwrap(),
    };
    let mut statement = memory_db.prepare("INSERT INTO prices VALUES (:date, :premise_code, :item_code, :price)").unwrap();
    statement.bind(&[(":date", temp.date)][..]).unwrap();
    statement.bind(&[(":premise_code", temp.premise_code)][..]).unwrap();
    statement.bind(&[(":item_code", temp.item_code)][..]).unwrap();
    statement.bind(&[(":price", temp.price)][..]).unwrap();
    statement.next().unwrap();
}

// https://storage.googleapis.com/dosm-public-pricecatcher/lookup_premise.parquet
// premise_code,premise,address,premise_type,state,district
pub struct Premise {
    pub premise_code: i64,
    pub premise: String,
    pub address: String,
    pub premise_type: String,
    pub state: String,
    pub district: String,
}

fn push_premise(record: Row, memory_db: &sqlite::Connection) {
    let u = String::from("UNKNOWN");
    let temp = Premise {
        premise_code: record.get_long(0).unwrap(),
        premise: record.get_string(1).unwrap_or_else(|_error| &u).trim().to_string(),
        address: record.get_string(2).unwrap_or_else(|_error| &u).trim().to_string(),
        premise_type: record.get_string(3).unwrap_or_else(|_error| &u).trim().to_string(),
        state: record.get_string(4).unwrap_or_else(|_error| &u).trim().to_string(),
        district: record.get_string(5).unwrap_or_else(|_error| &u).trim().to_string(),
    };
    let mut statement = memory_db.prepare("INSERT INTO premises VALUES (:premise_code, :premise, :address, :premise_type, :state, :district)").unwrap();
    statement.bind(&[(":premise_code", temp.premise_code)][..]).unwrap();
    statement.bind(&[(":premise", temp.premise.trim())][..]).unwrap();
    statement.bind(&[(":address", temp.address.trim())][..]).unwrap();
    statement.bind(&[(":premise_type", temp.premise_type.trim())][..]).unwrap();
    statement.bind(&[(":state", temp.state.trim())][..]).unwrap();
    statement.bind(&[(":district", temp.district.trim())][..]).unwrap();
    statement.next().unwrap();
}

// https://storage.googleapis.com/dosm-public-pricecatcher/lookup_item.parquet
// item_code,item,unit,item_group,item_category
pub struct Item {
    pub item_code: i64,
    pub item: String,
    pub unit: String,
    pub item_group: String,
    pub item_category: String,
}

fn push_item(record: Row, memory_db: &sqlite::Connection) {
    let u = String::from("UNKNOWN");
    let temp = Item {
        item_code: record.get_long(0).unwrap(),
        item: record.get_string(1).unwrap_or_else(|_error| &u).trim().to_string(),
        unit: record.get_string(2).unwrap_or_else(|_error| &u).trim().to_string(),
        item_group: record.get_string(3).unwrap_or_else(|_error| &u).trim().to_string(),
        item_category: record.get_string(4).unwrap_or_else(|_error| &u).trim().to_string(),
    };
    let mut statement = memory_db.prepare("INSERT INTO items VALUES (:item_code, :item, :unit, :item_group, :item_category)").unwrap();
    statement.bind(&[(":item_code", temp.item_code)][..]).unwrap();
    statement.bind(&[(":item", temp.item.trim())][..]).unwrap();
    statement.bind(&[(":unit", temp.unit.trim())][..]).unwrap();
    statement.bind(&[(":item_group", temp.item_group.trim())][..]).unwrap();
    statement.bind(&[(":item_category", temp.item_category.trim())][..]).unwrap();
    statement.next().unwrap();
}

fn execute(handler: fn(Row, &sqlite::Connection), file_path: &str, memory_db: &sqlite::Connection) {
    let path = Path::new(file_path);
    let file = File::open(&path).unwrap_or_else(|error| {
        panic!("Problem opening the file: {:?}", error);
    });
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        handler(record, memory_db);
    }
}

fn main() {

    let memory_db = sqlite::open(":memory:").unwrap();
    let sql_blueprint = "
        CREATE TABLE prices (date INTEGER, premise_code INTEGER, item_code INTEGER, price INTEGER);
        CREATE TABLE premises (premise_code INTEGER, premise TEXT, address TEXT, premise_type TEXT, state TEXT, district TEXT);
        CREATE TABLE items (item_code INTEGER, item TEXT, unit TEXT, item_group TEXT, item_category TEXT);
    ";
    memory_db.execute(sql_blueprint).unwrap();

    execute(push_price, "/home/arma7x/Downloads/opendosm/pricecatcher_2022-01.parquet", &memory_db);
    execute(push_premise, "/home/arma7x/Downloads/opendosm/lookup_premise.parquet", &memory_db);
    execute(push_item, "/home/arma7x/Downloads/opendosm/lookup_item.parquet", &memory_db);

    let query = "SELECT COUNT(*) as total FROM prices";
    memory_db
    .iterate(query, |pairs| {
        for &(name, value) in pairs.iter() {
            println!("{} = {}", name, value.unwrap());
        }
        true
    })
    .unwrap();
    println!("------------");
    let query = "SELECT COUNT(*) as total FROM premises";
    memory_db
    .iterate(query, |pairs| {
        for &(name, value) in pairs.iter() {
            println!("{} = {}", name, value.unwrap());
        }
        true
    })
    .unwrap();
    println!("------------");
    let query = "SELECT COUNT(*) as total FROM items";
    memory_db
    .iterate(query, |pairs| {
        for &(name, value) in pairs.iter() {
            println!("{} = {}", name, value.unwrap());
        }
        true
    })
    .unwrap();

    unsafe {
        let name = "main";
        let mut rc: c_int = 0;

        let c_str = CString::new(name).unwrap();
        let main: *const c_char = c_str.as_ptr() as *const c_char;

        let backup_memory_db = sqlite::open("/home/arma7x/Downloads/opendosm/backup.db").unwrap();
        backup_memory_db.execute(sql_blueprint).unwrap();

        let p_backup = sqlite3_sys::sqlite3_backup_init(backup_memory_db.as_raw(), main, memory_db.as_raw(), main);
        loop {
            println!("Status begin:{:?}", rc);
            println!("Progress {:?}, {:?}", sqlite3_sys::sqlite3_backup_remaining(p_backup), sqlite3_sys::sqlite3_backup_pagecount(p_backup));
            rc = sqlite3_sys::sqlite3_backup_step(p_backup, 100);
            println!("Status end :{:?}", rc);
            if rc == sqlite3_sys::SQLITE_OK || rc == sqlite3_sys::SQLITE_BUSY || rc == sqlite3_sys::SQLITE_LOCKED {
              sqlite3_sys::sqlite3_sleep(250);
            } else {
                break;
            }
        }
    }
}
