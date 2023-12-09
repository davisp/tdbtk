// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::Result;

use crate::filters::Filter;
use crate::storage;

#[derive(Default)]
pub struct EmptyFilter {}

impl EmptyFilter {
    pub fn from_config(config: &storage::FilterConfig) -> Self {
        match config {
            storage::FilterConfig::None => EmptyFilter::default(),
            _ => panic!("Invalid filter config for empty filter: {:?}", config),
        }
    }
}

impl Filter for EmptyFilter {
    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        std::mem::swap(output, input);
        Ok(())
    }
}
