use std::default::Default;

use anyhow::{anyhow, Result};
use miniz_oxide::deflate::compress_to_vec_zlib;
use miniz_oxide::inflate::decompress_to_vec_zlib;

use crate::filters::compressors::Compressor;

pub struct GZIPCompressor {
    level: u8,
}

impl GZIPCompressor {
    fn new(level: u8) -> Self {
        GZIPCompressor { level }
    }
}

impl Default for GZIPCompressor {
    fn default() -> Self {
        GZIPCompressor { level: 6 }
    }
}

impl Compressor for GZIPCompressor {
    fn compress(&self, input: &[u8]) -> Vec<u8> {
        compress_to_vec_zlib(input, self.level)
    }

    fn decompress(&self, input: &[u8]) -> Result<Vec<u8>> {
        decompress_to_vec_zlib(input).map_err(|err| {
            let context = format!("{}", err);
            anyhow!("Error ").context(context)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_decompression() {
        let data = "Hello, world!";

        let compressor = GZIPCompressor::default();
        let compressed = compressor.compress(data.as_bytes());
        let decompressed =
            compressor.decompress(&compressed).unwrap_or_else(|err| {
                panic!("Failed to compress gzip data: {}", err)
            });
        assert_eq!(decompressed, data.as_bytes());
    }

    #[test]
    fn non_default_level() {
        let data = "Hello, world!";

        let compressor = GZIPCompressor::new(0);
        let compressed = compressor.compress(data.as_bytes());
        let decompressed =
            compressor.decompress(&compressed).unwrap_or_else(|err| {
                panic!("Failed to decompress gzip data: {}", err);
            });
        assert_eq!(decompressed, data.as_bytes());
    }
}
