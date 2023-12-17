// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::{anyhow, Result};
use zstd_safe;

use crate::filters;
use crate::filters::compression;
use crate::storage;

pub struct LZ4Filter {
    level: i32,
}

impl LZ4Filter {
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
            if matches!(ctype, filters::FilterType::LZ4) {
                return Ok(Box::from(LZ4Filter::new(*level)));
            }
        }

        Err(anyhow!("Invalid filter config {:?} for LZ4Filter", config))
    }

    pub fn decompress(&self, input: &[u8], output: &mut [u8]) -> Result<()> {
        zstd_safe::decompress(output, input).map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error decompressing lz4 data").context(context)
        })?;
        Ok(())
    }
}

impl filters::Filter for LZ4Filter {
    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        compression::decompress(&|i, o| self.decompress(i, o), input, output)
    }
}
