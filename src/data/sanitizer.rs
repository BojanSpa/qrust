use std::fs::OpenOptions;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;

use csv::Reader as CsvReader;
use csv::StringRecord as CsvStringRecord;
use csv::Writer as CsvWriter;

pub struct CsvSanitizer;
impl CsvSanitizer {
    pub fn new() -> CsvSanitizer {
        CsvSanitizer {}
    }

    pub fn cleanup(&self, csvpath: &Path) -> Result<(), IoError> {
        if !self.is_csv(csvpath) {
            return Err(IoError::new(ErrorKind::InvalidInput, "Not a CSV file."));
        }

        let mut content_records = CsvReader::from_path(csvpath)?
            .into_records()
            .map(|rec| rec.unwrap())
            .collect::<Vec<CsvStringRecord>>();

        let first_record = match content_records.first() {
            Some(rec) => rec,
            None => {
                log::warn!("CSV file is empty.");
                return Ok(());
            }
        };

        let header_record = self.get_header_record();
        if first_record == &header_record {
            return Ok(());
        };

        content_records.insert(0, header_record);

        let csv_file = OpenOptions::new().read(true).write(true).open(csvpath)?;
        let mut writer = CsvWriter::from_writer(csv_file);
        for record in content_records {
            writer.write_record(&record)?;
        }
        writer.flush()?;

        Ok(())
    }

    fn is_csv(&self, path: &Path) -> bool {
        const CSV_EXT: &str = "csv";
        return match path.extension() {
            Some(ext) => ext == CSV_EXT,
            None => false,
        };
    }

    fn get_header_record(&self) -> CsvStringRecord {
        const HEADER: &str = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore";
        let header_parts = HEADER.split(',').collect::<Vec<&str>>();
        CsvStringRecord::from(header_parts)
    }
}

// pub struct DataFrameSanitizer;
// impl DataFrameSanitizer {
//     pub fn new() -> DataFrameSanitizer {
//         DataFrameSanitizer {}
//     }
// }
