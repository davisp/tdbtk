// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::Result;

use crate::filters::compressors::Compressor;
use crate::filters::Filter;

pub struct CompressionFilter {
    compressor: Box<dyn Compressor>,
}

impl CompressionFilter {
    fn new(compressor: Box<dyn Compressor>) -> CompressionFilter {
        CompressionFilter { compressor }
    }
}

impl Filter for CompressionFilter {
    type InputItem = u8;
    type OutputItem = u8;

    fn filter(&self, input: &[u8]) -> Vec<u8> {
        self.compressor.compress(input)
    }

    fn unfilter(&self, input: &[u8]) -> Result<Vec<u8>> {
        self.compressor.decompress(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::compressors::gzip::GZIPCompressor;

    #[test]
    fn basic_filter() {
        let data = "Hello, world!";

        let g: Box<GZIPCompressor> = Box::default();
        let f = CompressionFilter::new(g);

        let filtered = f.filter(data.as_bytes());
        let unfiltered = f.unfilter(&filtered).unwrap_or_else(|err| {
            panic!("Error unfiltering gzip data! {}", err);
        });

        assert_eq!(unfiltered, data.as_bytes());
    }
}
