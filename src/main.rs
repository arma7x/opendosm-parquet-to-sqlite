use reqwest;
use bytes::Bytes;
use sqlite;
use libc::{c_char, c_int};
use sqlite3_sys;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor, RowFormatter};
use std::{fs::File};
use std::io::{Error, ErrorKind, Cursor, copy, stdin};
use std::env;
use std::ffi::CString;
use std::io::prelude::*;
use scraper::{Html, Selector};
use xz2::read::{XzEncoder, XzDecoder};

// date,premise_code,item_code,price
fn push_pricecatcher(record: Row, memory_db: &sqlite::Connection) {
    let mut statement = memory_db.prepare("INSERT INTO prices VALUES (:date, :premise_code, :item_code, :price)").unwrap();
    statement.bind(&[(":date", record.fmt(0).to_string()[..10].to_string().trim())][..]).unwrap();
    statement.bind(&[(":premise_code", record.fmt(1).to_string().parse::<i64>().unwrap())][..]).unwrap();
    statement.bind(&[(":item_code", record.fmt(2).to_string().parse::<i64>().unwrap())][..]).unwrap();
    statement.bind(&[(":price", record.fmt(3).to_string().parse::<f64>().unwrap())][..]).unwrap();
    statement.next().unwrap();
}

// premise_code,premise,address,premise_type,state,district
fn push_premise(record: Row, memory_db: &sqlite::Connection) {
    let u = String::from("UNKNOWN");
    let mut statement = memory_db.prepare("INSERT INTO premises VALUES (:premise_code, :premise, :address, :premise_type, :state, :district)").unwrap();
    statement.bind(&[(":premise_code", record.fmt(0).to_string().parse::<i64>().unwrap())][..]).unwrap();
    statement.bind(&[(":premise", record.get_string(1).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":address", record.get_string(2).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":premise_type", record.get_string(3).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":state", record.get_string(4).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":district", record.get_string(5).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.next().unwrap();
}

// item_code,item,unit,item_group,item_category
fn push_item(record: Row, memory_db: &sqlite::Connection) {
    let u = String::from("UNKNOWN");
    let mut statement = memory_db.prepare("INSERT INTO items VALUES (:item_code, :item, :unit, :item_group, :item_category)").unwrap();
    statement.bind(&[(":item_code", record.fmt(0).to_string().parse::<i64>().unwrap())][..]).unwrap();
    statement.bind(&[(":item", record.get_string(1).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":unit", record.get_string(2).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":item_group", record.get_string(3).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.bind(&[(":item_category", record.get_string(4).unwrap_or_else(|_error| &u).trim())][..]).unwrap();
    statement.next().unwrap();
}

fn execute(handler: fn(Row, &sqlite::Connection), file: File, memory_db: &sqlite::Connection) {
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    while let Some(record) = iter.next() {
        handler(record, memory_db);
    }
}

fn get_pricecatcher_records() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    println!("Fetching pricecatcher records:  {}", "https://open.dosm.gov.my/data-catalogue");
    let response = reqwest::blocking::get("https://open.dosm.gov.my/data-catalogue")?.text()?;
    let document = scraper::Html::parse_document(&response);
    let sections = Selector::parse("section")?;
    let mut li_string = String::new();
    for element in document.select(&sections) {
        if element.text().nth(0).unwrap().trim() == "Economy: PriceCatcher" {
            li_string = String::from(element.inner_html());
            break;
        }
    }
    if li_string == "" {
        return Err(String::from("No options available").into());
    }
    let mut records: Vec<String> = vec![];
    let fragment = Html::parse_fragment(&li_string);
    let selector = Selector::parse("li").unwrap();
    for element in fragment.select(&selector) {
        let texts: Vec<&str> = element.text().nth(0).unwrap().trim().split(" ").collect();
        if texts.len() == 2 && texts[0] == "PriceCatcher:" {
            records.push(texts[1].replace("/", "-").to_string());
        }
    }
    Ok(records)
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

fn get_file_as_byte_vec(file_path: &str) -> Result<Vec<u8>, Error> {
    let mut f = File::open(file_path)?;
    let metadata = std::fs::metadata(file_path)?;
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer)?;
    Ok(buffer)
}

fn main() {

    let records = get_pricecatcher_records().unwrap();
    for (i, value) in records.iter().enumerate() {
        println!("{} => {}", value, i);
    }
    println!("");
    let mut len_or_choice = records.len() as u32;

    loop {
        println!("Please enter your choice:");
        let mut num = String::new();
        stdin().read_line(&mut num).expect("Failed to read line");
        let num: u32 = match num.trim().parse() {
            Ok(num) => num,
            Err(_) => continue,
        };
        if num > len_or_choice - 1 {
            println!("{num} is invalid");
            continue;
        } else {
            len_or_choice = num;
            break;
        }
    }

    let date = records[len_or_choice as usize].as_str();
    println!("You choice: {}", date);

    let memory_db = sqlite::open(":memory:").unwrap();
    let sql_blueprint = "
        CREATE TABLE prices (date TEXT, premise_code INTEGER, item_code INTEGER, price INTEGER);
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

    let pricecatcher_parquet_url = format!("https://storage.googleapis.com/dosm-public-pricecatcher/pricecatcher_{}.parquet", date);
    let pricecatcher_parquet_url = pricecatcher_parquet_url.as_str();
    let mut pricecatcher_parquet = base_path.clone();
    pricecatcher_parquet.push(format!("pricecatcher_{}.parquet", date));
    let pricecatcher_parquet_file = get_file(pricecatcher_parquet.into_os_string().to_str().unwrap(), pricecatcher_parquet_url).unwrap();
    tasks.push((push_pricecatcher, pricecatcher_parquet_file, &memory_db));

    println!("Build database...");
    for (handler, file, database_conn) in tasks {
        execute(handler, file, database_conn);
    }
    println!("Build database, DONE!");

    let mut backup_path = base_path.clone();
    backup_path.push(format!("pricecatcher_{}.db", date));
    let backup_path_str = backup_path.into_os_string();

    unsafe {
        let mut rc: c_int = 0;

        let c_str = CString::new("main").unwrap();
        let main: *const c_char = c_str.as_ptr() as *const c_char;
        std::fs::remove_file(backup_path_str.to_str().unwrap());
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
    }
    let buffer = get_file_as_byte_vec(backup_path_str.to_str().unwrap()).unwrap();
    let compressor = XzEncoder::new(&buffer[..], 9);
    let mut decompressor = XzDecoder::new(compressor);
    println!("Backup path: {}", backup_path_str.to_str().unwrap());
}
