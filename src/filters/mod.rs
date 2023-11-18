use anyhow::Result;

pub mod compression;
mod compressors;

#[repr(u8)]
pub enum FilterType {
    None = 0,
    GZip = 1,
    Zstd = 2,
    LZ4 = 3,
    Rle = 4,
    BZip2 = 5,
    DoubleDelta = 6,
    BitWdithReduction = 7,
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
