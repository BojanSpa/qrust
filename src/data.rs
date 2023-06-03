pub mod config;
pub mod provider;
pub mod sanitizer;
pub mod store;

pub enum AssetCategory {
    Spot,
    Usdm,
    Coinm,
}
impl AssetCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetCategory::Spot => "spot",
            AssetCategory::Usdm => "um",
            AssetCategory::Coinm => "cm",
        }
    }

    pub fn as_value(value: &str) -> AssetCategory {
        match value {
            "spot" => AssetCategory::Spot,
            "um" => AssetCategory::Usdm,
            "cm" => AssetCategory::Coinm,
            _ => panic!("Invalid asset category: {}", value),
        }
    }
}
