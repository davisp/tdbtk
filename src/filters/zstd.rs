// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::{anyhow, Result};
use zstd_safe;

use crate::filters;
use crate::filters::compression;
use crate::storage;

pub struct ZstdFilter {
    level: i32,
}

impl ZstdFilter {
    fn new(level: i32) -> Self {
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
            if matches!(ctype, filters::FilterType::Zstd) {
                let level = if *level >= 1 && *level <= 22 {
                    *level
                } else {
                    3
                };
                return Ok(Box::from(ZstdFilter::new(level)));
            }
        }

        Err(anyhow!("Invalid filter config {:?} for ZstdFilter", config))
    }

    pub fn decompress(&self, input: &[u8], output: &mut [u8]) -> Result<()> {
        zstd_safe::decompress(output, input).map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error decompressing zstd data").context(context)
        })?;
        Ok(())
    }
}

impl filters::Filter for ZstdFilter {
    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        compression::decompress(&|i, o| self.decompress(i, o), input, output)
    }
}
