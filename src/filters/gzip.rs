// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::cmp;
use std::default::Default;

use anyhow::{anyhow, Result};
use binrw::io::Cursor;
use binrw::BinRead;
use miniz_oxide::deflate;
use miniz_oxide::inflate;

use crate::filters::Filter;
use crate::storage;

pub struct GZipFilter {
    level: u8,
}

impl GZipFilter {
    fn new(level: u8) -> Self {
        GZipFilter { level }
    }

    pub fn from_config(config: &storage::FilterConfig) -> Self {
        match config {
            storage::FilterConfig::Compression {
                compressor_type: _,
                compression_level,
                reinterpret_type: _,
            } => {
                if compression_level >= &0 {
                    GZipFilter::new(*cmp::min(compression_level, &10) as u8)
                } else {
                    GZipFilter::default()
                }
            }
            _ => panic!("Invalid filter config for gzip filter: {:?}", config),
        }
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

impl Default for GZipFilter {
    fn default() -> Self {
        GZipFilter { level: 6 }
    }
}

impl Filter for GZipFilter {
    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        let mut reader = Cursor::new(&(input.metadata));
        let comp_info = storage::CompressionChunks::read(&mut reader)?;

        // Create our metadata buffer
        let total_metadata_size: usize = comp_info
            .metadata_parts
            .iter()
            .map(|chunk| chunk.uncompressed_size as usize)
            .sum();
        output.metadata.resize(total_metadata_size, 0);

        // Create our data buffer
        let total_data_size: usize = comp_info
            .data_parts
            .iter()
            .map(|chunk| chunk.uncompressed_size as usize)
            .sum();
        output.data.resize(total_data_size, 0);

        let decompress = |chunk: &storage::CompressionChunkInfo,
                          output: &mut [u8],
                          input_offset: &mut usize,
                          output_offset: &mut usize|
         -> Result<()> {
            let input_end = *input_offset + chunk.compressed_size as usize;
            let output_end = *output_offset + chunk.uncompressed_size as usize;
            self.decompress(
                &(input.data)[*input_offset..input_end],
                &mut output[*output_offset..output_end],
            )?;
            *input_offset += chunk.compressed_size as usize;
            *output_offset += chunk.uncompressed_size as usize;
            Ok(())
        };

        // Track where we are in the input data buffer
        let mut input_offset = 0;

        // Track where we are in the output metadata buffer
        let mut output_offset = 0;

        // Decompress metadata chunks
        for chunk in comp_info.metadata_parts {
            decompress(
                &chunk,
                &mut output.metadata,
                &mut input_offset,
                &mut output_offset,
            )?
        }

        // Track where we are in the output data buffer
        let mut output_offset = 0;

        // Decompress data chunks
        for chunk in comp_info.data_parts {
            decompress(
                &chunk,
                &mut output.data,
                &mut input_offset,
                &mut output_offset,
            )?
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_decompression() {
        let data = "Hello, World!";

        let filter = GZipFilter::default();
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

        let filter = GZipFilter::default();
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
