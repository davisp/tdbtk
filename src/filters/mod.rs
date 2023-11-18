use anyhow::Result;

pub mod compression;
mod compressors;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum FilterType {
    #[default]
    None = 0,
    GZip = 1,
    Zstd = 2,
    LZ4 = 3,
    Rle = 4,
    BZip2 = 5,
    DoubleDelta = 6,
    BitWidthReduction = 7,
    BitShuffle = 8,
    ByteShuffle = 9,
    PositiveDelta = 10,
    Encryption = 11,
    ChecksumMD5 = 12,
    ChecksumSHA256 = 13,
    Dictionary = 14,
    ScaleFloat = 15,
    Xor = 16,
    Deprecated = 17,
    WebP = 18,
    Delta = 19,
    InvalidFilterType = 255,
}

impl From<u8> for FilterType {
    fn from(orig: u8) -> Self {
        match orig {
            0 => FilterType::None,
            1 => FilterType::GZip,
            2 => FilterType::Zstd,
            3 => FilterType::LZ4,
            4 => FilterType::Rle,
            5 => FilterType::BZip2,
            6 => FilterType::DoubleDelta,
            7 => FilterType::BitWidthReduction,
            8 => FilterType::BitShuffle,
            9 => FilterType::ByteShuffle,
            10 => FilterType::PositiveDelta,
            11 => FilterType::Encryption,
            12 => FilterType::ChecksumMD5,
            13 => FilterType::ChecksumSHA256,
            14 => FilterType::Dictionary,
            15 => FilterType::ScaleFloat,
            16 => FilterType::Xor,
            17 => FilterType::Deprecated,
            18 => FilterType::WebP,
            19 => FilterType::Delta,
            _ => return FilterType::InvalidFilterType,
        }
    }
}

pub trait Filter {
    type InputItem;
    type OutputItem;

    fn filter(&self, input: &[Self::InputItem]) -> Vec<Self::OutputItem>;
    fn unfilter(
        &self,
        input: &[Self::OutputItem],
    ) -> Result<Vec<Self::InputItem>>;
}
