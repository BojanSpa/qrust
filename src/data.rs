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
    pub fn to_str(&self) -> &'static str {
        match self {
            AssetCategory::Spot => "spot",
            AssetCategory::Usdm => "um",
            AssetCategory::Coinm => "cm",
        }
    }

    pub fn by_str(category: &str) -> AssetCategory {
        match category {
            "spot" => AssetCategory::Spot,
            "um" => AssetCategory::Usdm,
            "cm" => AssetCategory::Coinm,
            _ => panic!("Invalid asset category: {}", category),
        }
    }
}
