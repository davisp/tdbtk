// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::{anyhow, Result};
use miniz_oxide::deflate;
use miniz_oxide::inflate;

use crate::filters;
use crate::filters::compression;
use crate::storage;

pub struct GZipFilter {
    level: u8,
}

impl GZipFilter {
    fn new(level: u8) -> Self {
        Self { level }
    }

    pub fn from_config(
        config: &storage::FilterConfig,
    ) -> Result<Box<dyn filters::Filter>> {
        if let storage::FilterConfig::Compression {
            compressor_type: ctype,
            compression_level: level,
            reinterpret_type: _,
        } = config
        {
            if matches!(ctype, filters::FilterType::GZip) && *level < 10 {
                return Ok(Box::from(GZipFilter::new(*level as u8)));
            }
        }

        Err(anyhow!("Invalid config {:?} for GzipFilter", config))
    }

    pub fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        let compressed = deflate::compress_to_vec_zlib(input, self.level);
        output.resize(compressed.len(), 0);
        output.copy_from_slice(&compressed);
        Ok(())
    }

    pub fn decompress(&self, input: &[u8], output: &mut [u8]) -> Result<()> {
        inflate::decompress_slice_iter_to_slice(
            output,
            [input].iter().copied(),
            true,
            false,
        )
        .map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error decompressing gzip data").context(context)
        })?;
        Ok(())
    }
}

impl filters::Filter for GZipFilter {
    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        compression::decompress(&|i, o| self.decompress(i, o), input, output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_decompression() {
        let data = "Hello, World!";

        let filter = GZipFilter::new(6);
        let mut unfiltered = data.as_bytes().to_vec();
        let mut filtered = Vec::new();

        filter
            .compress(&unfiltered, &mut filtered)
            .unwrap_or_else(|err| {
                panic!("Failed to gzip compress buffer: {:?}", err);
            });

        assert!(!filtered.is_empty());
        assert_ne!(filtered, data.as_bytes().to_vec());

        unfiltered.clear();
        assert!(unfiltered.is_empty());
        assert_ne!(unfiltered, data.as_bytes().to_vec());

        // Resize our output buffer to accept the decompressed data.
        unfiltered.resize(data.len(), 0);

        filter
            .decompress(&filtered, &mut unfiltered)
            .unwrap_or_else(|err| {
                panic!("Failed to gzip decompress buffer: {:?}", err);
            });

        assert_eq!(unfiltered, data.as_bytes().to_vec());
    }

    #[test]
    fn non_default_level() {
        let data = "Hello, World!";

        let filter = GZipFilter::new(6);
        let mut unfiltered = data.as_bytes().to_vec();
        let mut filtered = Vec::new();

        filter
            .compress(&unfiltered, &mut filtered)
            .unwrap_or_else(|err| {
                panic!("Failed to gzip compress buffer: {:?}", err);
            });

        assert!(!filtered.is_empty());
        assert_ne!(filtered, data.as_bytes().to_vec());

        unfiltered.clear();
        assert!(unfiltered.is_empty());
        assert_ne!(unfiltered, data.as_bytes().to_vec());

        // Resize our output buffer to accept the decompressed data.
        unfiltered.resize(data.len(), 0);

        filter
            .decompress(&filtered, &mut unfiltered)
            .unwrap_or_else(|err| {
                panic!("Failed to gzip decompress buffer: {:?}", err);
            });

        assert_eq!(unfiltered, data.as_bytes().to_vec());
    }
}
