// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::{anyhow, Result};

use crate::filters;
use crate::storage;

#[derive(Default)]
pub struct EmptyFilter {}

impl EmptyFilter {
    pub fn from_config(
        config: &storage::FilterConfig,
    ) -> Result<Box<dyn filters::Filter>> {
        match config {
            storage::FilterConfig::None => {
                return Ok(Box::from(EmptyFilter {}));
            }
            _ => {}
        }

        Err(anyhow!("Invalid config {:?} for EmptyFilter", config))
    }
}

impl filters::Filter for EmptyFilter {
    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        std::mem::swap(output, input);
        Ok(())
    }
}
