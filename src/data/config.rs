pub struct DataConfig<'a> {
    pub exchange: &'a str,
    pub exchange_info_uri: &'a str,
    pub klines_uri: &'a str,
    pub hist_klines_monthly_uri: &'a str,
    pub hist_klines_daily_uri: &'a str,

    pub base_raw_dir: &'a str,
    pub base_store_dir: &'a str,

    pub file_format: &'a str,
    pub date_format_monthly: &'a str,
    pub date_format_daily: &'a str,
}
