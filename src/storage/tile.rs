// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use binrw::binrw;

pub const GENERIC_TILE_HEADER_SIZE: u64 = 34;

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
    pub version: u32,
    pub persisted_size: u64,
    pub tile_size: u64,
    pub datatype: u8,
    pub cell_size: u64,
    pub encryption_type: u8,
    pub filter_pipeline_size: u32,
}
