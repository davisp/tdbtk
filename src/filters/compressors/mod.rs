// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::Result;

pub mod gzip;

pub trait Compressor {
    fn compress(&self, input: &[u8]) -> Vec<u8>;
    fn decompress(&self, input: &[u8]) -> Result<Vec<u8>>;
}
