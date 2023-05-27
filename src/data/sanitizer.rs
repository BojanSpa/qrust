use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;

const HEADER: &str = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore";

pub struct CsvSanitizer;
impl CsvSanitizer {
    pub fn new() -> CsvSanitizer {
        return CsvSanitizer {};
    }

    pub fn cleanup(&self, csv_path: &Path) -> Result<(), IoError> {
        if !self.is_csv(csv_path) {
            return Err(IoError::new(ErrorKind::InvalidInput, "Not a CSV file."));
        }

        return Ok(());
    }

    fn is_csv(&self, path: &Path) -> bool {
        const CSV_EXT: &str = ".csv";

        return match path.extension() {
            Some(ext) => ext == CSV_EXT,
            None => false,
        };
    }
}
