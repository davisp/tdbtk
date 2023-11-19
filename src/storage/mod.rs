// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use binrw::binrw;

use crate::filters::FilterType;

mod schema;

pub const CURRENT_FORMAT_VERSION: u32 = 21;
pub const GENERIC_TILE_HEADER_SIZE: u64 = 34;

fn is_compression_filter(ftype: FilterType) -> bool {
    matches!(
        ftype as FilterType,
        FilterType::GZip
            | FilterType::Zstd
            | FilterType::Rle
            | FilterType::BZip2
            | FilterType::Delta
            | FilterType::DoubleDelta
            | FilterType::Dictionary
    )
}

fn is_bit_width_reduction_filter(ftype: FilterType) -> bool {
    matches!(ftype, FilterType::BitWidthReduction)
}

fn is_positive_delta_filter(ftype: FilterType) -> bool {
    matches!(ftype, FilterType::PositiveDelta)
}

fn is_scale_float_filter(ftype: FilterType) -> bool {
    matches!(ftype, FilterType::ScaleFloat)
}

fn is_webp_filter(ftype: FilterType) -> bool {
    matches!(ftype, FilterType::WebP)
}

fn is_no_config_filter(ftype: FilterType) -> bool {
    !(is_compression_filter(ftype)
        || is_bit_width_reduction_filter(ftype)
        || is_positive_delta_filter(ftype)
        || is_scale_float_filter(ftype)
        || is_webp_filter(ftype))
}

#[derive(Debug, Default)]
#[binrw]
#[brw(little)]
#[br(import { filter_type: FilterType })]
pub enum FilterConfig {
    #[br(pre_assert(is_compression_filter(filter_type)))]
    Compression {
        #[br(map = |ftype: u8| ftype.into())]
        #[bw(map = |ftype: &FilterType| *ftype as u8)]
        compressor_type: FilterType,
        compression_level: i32,
        #[br(if(matches!(compressor_type,
            FilterType::Delta | FilterType::DoubleDelta)))]
        reinterpret_type: u8,
    },
    #[br(pre_assert(is_bit_width_reduction_filter(filter_type)))]
    BitWidthReduction { max_window_size: u32 },
    #[br(pre_assert(is_positive_delta_filter(filter_type)))]
    PositiveDelta { max_window_size: u32 },
    #[br(pre_assert(is_scale_float_filter(filter_type)))]
    ScaleFloat {
        scale: f64,
        offset: f64,
        byte_width: u64,
    },
    #[br(pre_assert(is_webp_filter(filter_type)))]
    WebP {
        quality: f32,
        format: u8,
        lossless: u8,
        y_extent: u16,
        x_extent: u16,
    },
    #[default]
    #[br(pre_assert(is_no_config_filter(filter_type)))]
    None,
}

#[derive(Debug, Default)]
#[binrw]
#[brw(little)]
pub struct Filter {
    #[br(map = |ftype: u8| ftype.into())]
    #[bw(map = |ftype: &FilterType| *ftype as u8)]
    #[brw(assert(!matches!(filter_type, FilterType::Invalid)))]
    filter_type: FilterType,
    metadata_len: u32,

    #[br(args { filter_type })]
    config: FilterConfig,
}

impl Filter {
    pub fn filter_type(&self) -> FilterType {
        self.filter_type
    }

    pub fn config(&self) -> &FilterConfig {
        &self.config
    }
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct FilterList {
    max_chunk_size: u32,
    num_filters: u32,

    #[br(count(num_filters))]
    filters: Vec<Filter>,
}

impl FilterList {
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }
}

#[derive(Debug, Default)]
#[binrw]
#[brw(little)]
pub struct Chunk {
    pub original_size: u32,
    data_size: u32,
    metadata_size: u32,

    #[br(count(metadata_size))]
    pub metadata: Vec<u8>,

    #[br(count(data_size))]
    pub data: Vec<u8>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct ChunkedData {
    pub num_chunks: u64,

    #[br(count(num_chunks))]
    pub chunks: Vec<Chunk>,
}

impl ChunkedData {
    pub fn new(num_chunks: u64) -> Self {
        let mut chunks = Vec::new();
        chunks.resize_with(num_chunks as usize, Chunk::default);
        ChunkedData { num_chunks, chunks }
    }
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct CompressionChunkInfo {
    pub uncompressed_size: u32,
    pub compressed_size: u32,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct CompressionChunks {
    num_metadata_parts: u32,
    num_data_parts: u32,

    #[br(count(num_metadata_parts))]
    pub metadata_parts: Vec<CompressionChunkInfo>,

    #[br(count(num_data_parts))]
    pub data_parts: Vec<CompressionChunkInfo>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct GenericTileHeader {
    version: u32,
    persisted_size: u64,
    tile_size: u64,
    datatype: u8,
    cell_size: u64,
    encryption_type: u8,
    filter_pipeline_size: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::FilterChain;
    use binrw::io::Cursor;
    use binrw::BinRead;
    use util::read_test_file_at;

    #[test]
    fn basic_read() {
        let header_data = read_test_file_at(
            "resources/schema/schema_1".to_string(),
            GENERIC_TILE_HEADER_SIZE,
            0,
        );
        let mut reader = Cursor::new(header_data);
        let header = GenericTileHeader::read(&mut reader).unwrap();
        println!("{:?}", header);

        let pipeline_data = read_test_file_at(
            "resources/schema/schema_1".to_string(),
            header.filter_pipeline_size as u64,
            GENERIC_TILE_HEADER_SIZE,
        );
        let mut reader = Cursor::new(pipeline_data);
        let pipeline = FilterList::read(&mut reader).unwrap();
        println!("{:?}", pipeline);

        let chain = FilterChain::from_list(&pipeline);

        let disk_data = read_test_file_at(
            "resources/schema/schema_1".to_string(),
            header.persisted_size,
            GENERIC_TILE_HEADER_SIZE + header.filter_pipeline_size as u64,
        );

        let mut reader = Cursor::new(disk_data);
        let mut chunks = ChunkedData::read(&mut reader).unwrap();

        let data = chain.unfilter_chunks(&mut chunks).unwrap_or_else(|err| {
            panic!("Failed to unfilter tile data: {:?}", err);
        });

        println!("Data! {:?}", data);

        // let mut unfiltered = Vec::new();
        // chain
        //     .unfilter(&mut tile_data, &mut unfiltered)
        //     .unwrap_or_else(|err| {
        //         panic!("Failed to unfilter tile data: {:?}", err);
        //     });
    }
}
