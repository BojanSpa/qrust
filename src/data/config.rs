pub struct DataConfig {
    pub exchange: String,
    pub exchange_info_uri: String,
    pub klines_uri: String,
    pub hist_klines_monthly_uri: String,
    pub hist_klines_daily_uri: String,

    pub raw_dir: String,
    pub store_dir: String,

    pub file_format: String,
    pub date_format_monthly: String,
    pub date_format_daily: String,
}
