use std::fs::OpenOptions;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{anyhow, Result};
use csv::Reader as CsvReader;
use csv::StringRecord as CsvStringRecord;
use csv::Writer as CsvWriter;

pub struct CsvSanitizer;
impl CsvSanitizer {
    const HEADER: &str = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore";

    pub fn new() -> CsvSanitizer {
        CsvSanitizer {}
    }

    pub fn run(&self, path: &Path) -> Result<()> {
        self.check_csv(path)?;

        let mut records = self.get_records(path)?;
        self.check_if_empty(path, &records)?;
        self.check_header(&mut records);
        self.remove_invalid_records(&mut records);
        self.write_records(path, &mut records)?;

        Ok(())
    }

    fn check_csv(&self, path: &Path) -> Result<()> {
        const CSV_EXT: &str = "csv";

        let file_ext = path.extension().unwrap_or_default();
        if file_ext != CSV_EXT {
            return Err(IoError::new(ErrorKind::InvalidInput, "Not a CSV file.").into());
        }

        Ok(())
    }

    fn get_records(&self, path: &Path) -> Result<Vec<CsvStringRecord>> {
        let reader = CsvReader::from_path(path)?;
        let records = reader.into_records().collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    fn check_if_empty(&self, path: &Path, records: &Vec<CsvStringRecord>) -> Result<()> {
        if records.is_empty() {
            return Err(anyhow!("CSV file '{}' is empty.", path.display()));
        }
        Ok(())
    }

    fn check_header(&self, records: &mut Vec<CsvStringRecord>) {
        let first_record = records.first().unwrap();
        let header_record = self.get_header_record();

        if first_record != &header_record {
            records.insert(0, header_record);
        }
    }

    fn get_header_record(&self) -> CsvStringRecord {
        let header_parts = Self::HEADER.split(',').collect::<Vec<&str>>();
        CsvStringRecord::from(header_parts)
    }

    fn remove_invalid_records(&self, records: &mut Vec<CsvStringRecord>) {
        let header_len = records.first().unwrap().len();
        records.retain(|record| record.len() == header_len);
    }

    fn write_records(&self, path: &Path, records: &mut [CsvStringRecord]) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .truncate(true)
            .write(true)
            .open(path)?;

        let mut writer = CsvWriter::from_writer(file);
        for record in records.iter_mut() {
            writer.write_record(record.iter())?;
        }
        writer.flush()?;

        Ok(())
    }
}

// pub struct DataFrameSanitizer;
// impl DataFrameSanitizer {
//     pub fn new() -> DataFrameSanitizer {
//         DataFrameSanitizer {}
//     }
// }
