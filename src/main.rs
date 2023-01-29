use chrono;
use chrono::Datelike;
use reqwest;
use bytes::Bytes;
use sqlite;
use libc::{c_char, c_int};
use sqlite3_sys;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor, RowFormatter};
use std::{fs::File};
use std::io::{Error, ErrorKind, Cursor, copy};
use std::env;
use std::ffi::CString;

// date,premise_code,item_code,price
pub struct Price {
    pub date: String,
    pub premise_code: i64,
    pub item_code: i64,
    pub price: f64,
}

fn push_pricecatcher(record: Row, memory_db: &sqlite::Connection) {
    let temp = Price {
        date: record.fmt(0).to_string(),
        premise_code: record.get_int(1).unwrap() as i64,
        item_code: record.get_int(2).unwrap() as i64,
        price: record.get_double(3).unwrap(),
    };
    let mut statement = memory_db.prepare("INSERT INTO prices VALUES (:date, :premise_code, :item_code, :price)").unwrap();
    statement.bind(&[(":date", temp.date.trim())][..]).unwrap();
    statement.bind(&[(":premise_code", temp.premise_code)][..]).unwrap();
    statement.bind(&[(":item_code", temp.item_code)][..]).unwrap();
    statement.bind(&[(":price", temp.price)][..]).unwrap();
    statement.next().unwrap();
}

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

fn execute(handler: fn(Row, &sqlite::Connection), file: File, memory_db: &sqlite::Connection) {
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        handler(record, memory_db);
    }
}

fn download_file(cloud_path: &str) -> Result<Bytes, reqwest::Error> {
    let client = reqwest::blocking::Client::builder().timeout(Some(std::time::Duration::from_secs(3600))).build().unwrap();
    let response_bytes = match client.get(cloud_path).send() {
        Ok(response) => response.bytes()?,
        Err(error) => return Err(error),
    };
    Ok(response_bytes)
}

fn get_file_latest_revision(cloud_path: &str) -> Result<u64, reqwest::Error> {
    let client = reqwest::blocking::Client::new();
    let response = match client.head(cloud_path).send() {
        Ok(response) => response,
        Err(error) => return Err(error),
    };
    let content_length = response.headers()["content-length"].to_str().unwrap().parse::<u64>().unwrap();
    Ok(content_length)
}

fn get_file(local_path: &str, cloud_path: &str) -> Result<File, Error> {
    let mut check_file_latest_revision = true;

    let mut parquet_file = match File::open(local_path) {
        Ok(file) => file,
        Err(error) => {
            if error.kind() == ErrorKind::NotFound {
                println!("Download: {}", cloud_path);
                check_file_latest_revision = false;
                let mut file = File::create(local_path)?;
                let mut content = Cursor::new(download_file(cloud_path).unwrap());
                copy(&mut content, &mut file)?;
                file
            } else {
                return Err(error);
            }
        },
    };

    if check_file_latest_revision && parquet_file.metadata().unwrap().len() != get_file_latest_revision(cloud_path).unwrap() {
        println!("Cached outdated, re-downloading: {}", cloud_path);
        std::fs::remove_file(local_path).unwrap();
        parquet_file = File::create(local_path)?;
        let mut content = Cursor::new(download_file(cloud_path).unwrap());
        copy(&mut content, &mut parquet_file)?;
    } else {
        println!("From Cached: {}", local_path);
    }

    Ok(parquet_file)
}

fn main() {

    let current_date = chrono::Utc::now();
    let year = current_date.year();
    let month = current_date.month();

    let memory_db = sqlite::open(":memory:").unwrap();
    let sql_blueprint = "
        CREATE TABLE prices (date INTEGER, premise_code INTEGER, item_code INTEGER, price INTEGER);
        CREATE TABLE premises (premise_code INTEGER, premise TEXT, address TEXT, premise_type TEXT, state TEXT, district TEXT);
        CREATE TABLE items (item_code INTEGER, item TEXT, unit TEXT, item_group TEXT, item_category TEXT);
    ";
    memory_db.execute(sql_blueprint).unwrap();

    let mut tasks: Vec<(fn(Row, &sqlite::Connection), File, &sqlite::Connection)> = vec![];

    let mut base_path = env::current_exe().unwrap();
    base_path.pop();

    let item_parquet_url = "https://storage.googleapis.com/dosm-public-pricecatcher/lookup_item.parquet";
    let mut item_parquet = base_path.clone();
    item_parquet.push("lookup_item.parquet");
    let item_parquet_file = get_file(item_parquet.into_os_string().to_str().unwrap(), item_parquet_url).unwrap();
    tasks.push((push_item, item_parquet_file, &memory_db));

    let premise_parquet_url = "https://storage.googleapis.com/dosm-public-pricecatcher/lookup_premise.parquet";
    let mut premise_parquet = base_path.clone();
    premise_parquet.push("lookup_premise.parquet");
    let premise_parquet_file = get_file(premise_parquet.into_os_string().to_str().unwrap(), premise_parquet_url).unwrap();
    tasks.push((push_premise, premise_parquet_file, &memory_db));

    let pricecatcher_parquet_url = format!("https://storage.googleapis.com/dosm-public-pricecatcher/pricecatcher_{}-{:02}.parquet", year, month);
    let pricecatcher_parquet_url = pricecatcher_parquet_url.as_str();
    let mut pricecatcher_parquet = base_path.clone();
    pricecatcher_parquet.push(format!("pricecatcher_{}-{:02}.parquet", year, month));
    let pricecatcher_parquet_file = get_file(pricecatcher_parquet.into_os_string().to_str().unwrap(), pricecatcher_parquet_url).unwrap();
    tasks.push((push_pricecatcher, pricecatcher_parquet_file, &memory_db));

    for (handler, file, database_conn) in tasks {
        execute(handler, file, database_conn);
    }

    unsafe {
        let mut rc: c_int = 0;

        let c_str = CString::new("main").unwrap();
        let main: *const c_char = c_str.as_ptr() as *const c_char;

        let mut backup_path = base_path.clone();
        backup_path.push(format!("pricecatcher_{}-{:02}_{}.db", year, month, current_date.format("%Y-%m-%d %H:%M:%S").to_string()));
        let backup_path_str = backup_path.into_os_string();
        File::create(backup_path_str.to_str().unwrap()).unwrap();
        let backup_memory_db = sqlite::open(backup_path_str.to_str().unwrap()).unwrap();
        backup_memory_db.execute(sql_blueprint).unwrap();

        let p_backup = sqlite3_sys::sqlite3_backup_init(backup_memory_db.as_raw(), main, memory_db.as_raw(), main);
        loop {
            rc = sqlite3_sys::sqlite3_backup_step(p_backup, 1000);
            println!("Progress : {:?}, {:?}", sqlite3_sys::sqlite3_backup_remaining(p_backup), sqlite3_sys::sqlite3_backup_pagecount(p_backup));
            if rc == sqlite3_sys::SQLITE_OK || rc == sqlite3_sys::SQLITE_BUSY || rc == sqlite3_sys::SQLITE_LOCKED {
              sqlite3_sys::sqlite3_sleep(250);
            } else {
                break;
            }
        }
        println!("Saved path: {}", backup_path_str.to_str().unwrap());
    }
}
