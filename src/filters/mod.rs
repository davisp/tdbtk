// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::convert::TryFrom;

use anyhow::{anyhow, Result};

use crate::storage;

mod compression;
mod empty;
mod gzip;
mod lz4;
mod zstd;

pub trait Filter {
    // fn from_config(
    //     config: &storage::FilterConfig,
    // ) -> Result<Box<dyn Filter>, anyhow::Error>;

    // fn filter(
    //     &self,
    //     input: &mut storage::Chunk,
    //     output: &mut storage::Chunk,
    // ) -> Result<()>;

    fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()>;
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum FilterType {
    #[default]
    None = 0,
    GZip = 1,
    Zstd = 2,
    LZ4 = 3,
    Rle = 4,
    BZip2 = 5,
    DoubleDelta = 6,
    BitWidthReduction = 7,
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
    Invalid = 255,
}

impl From<u8> for FilterType {
    fn from(orig: u8) -> Self {
        match orig {
            0 => FilterType::None,
            1 => FilterType::GZip,
            2 => FilterType::Zstd,
            3 => FilterType::LZ4,
            4 => FilterType::Rle,
            5 => FilterType::BZip2,
            6 => FilterType::DoubleDelta,
            7 => FilterType::BitWidthReduction,
            8 => FilterType::BitShuffle,
            9 => FilterType::ByteShuffle,
            10 => FilterType::PositiveDelta,
            11 => FilterType::Encryption,
            12 => FilterType::ChecksumMD5,
            13 => FilterType::ChecksumSHA256,
            14 => FilterType::Dictionary,
            15 => FilterType::ScaleFloat,
            16 => FilterType::Xor,
            17 => FilterType::Deprecated,
            18 => FilterType::WebP,
            19 => FilterType::Delta,
            _ => FilterType::Invalid,
        }
    }
}

impl TryFrom<&storage::Filter> for Box<dyn Filter> {
    type Error = anyhow::Error;

    fn try_from(f: &storage::Filter) -> Result<Box<dyn Filter>, Self::Error> {
        match f.filter_type() {
            FilterType::None => empty::EmptyFilter::from_config(f.config()),
            FilterType::GZip => gzip::GZipFilter::from_config(f.config()),
            FilterType::LZ4 => lz4::LZ4Filter::from_config(f.config()),
            FilterType::Zstd => zstd::ZstdFilter::from_config(f.config()),
            ftype => Err(anyhow!("Unsupported filter type: {:?}", ftype)),
        }
    }
}

pub struct FilterChain {
    filter: Box<dyn Filter>,
    next: Option<Box<FilterChain>>,
}

impl FilterChain {
    pub fn unfilter(
        &self,
        input: &mut storage::Chunk,
        output: &mut storage::Chunk,
    ) -> Result<()> {
        match &self.next {
            None => self.filter.unfilter(input, output)?,
            Some(next_filter) => {
                next_filter.unfilter(input, output)?;
                std::mem::swap(output, input);
                self.filter.unfilter(input, output)?;
            }
        };
        Ok(())
    }

    pub fn unfilter_chunks(
        &self,
        chunks: &mut storage::ChunkedData,
    ) -> Result<Vec<u8>> {
        let mut scratch = storage::ChunkedData::new(chunks.num_chunks);
        for (input, output) in
            chunks.chunks.iter_mut().zip(scratch.chunks.iter_mut())
        {
            self.unfilter(input, output)?
        }

        let output_size = chunks
            .chunks
            .iter()
            .map(|chunk| chunk.original_size as usize)
            .sum();

        let mut output = vec![0; output_size];

        let mut output_offset = 0;
        for (chunk_in, chunk_out) in
            chunks.chunks.iter().zip(scratch.chunks.iter())
        {
            let output_end = output_offset + chunk_in.original_size as usize;
            output[output_offset..output_end].copy_from_slice(&chunk_out.data);
            output_offset += chunk_in.original_size as usize;
        }

        Ok(output)
    }
}

impl TryFrom<&storage::FilterList> for Box<FilterChain> {
    type Error = anyhow::Error;
    fn try_from(
        list: &storage::FilterList,
    ) -> Result<Box<FilterChain>, Self::Error> {
        let mut chain = None;
        for filter in list.filters().iter().rev() {
            let next: Box<dyn Filter> = <_>::try_from(filter)?;
            chain = Some(Box::from(FilterChain {
                filter: next,
                next: chain,
            }));
        }

        match chain {
            Some(filter_chain) => Ok(filter_chain),
            None => {
                Err(anyhow!("Error creating filter chain from empty list."))
            }
        }
    }
}
