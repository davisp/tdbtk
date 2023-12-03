// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::anyhow;
use binrw::io::Cursor;
use binrw::{binrw, BinRead};

use crate::filters;
use crate::io::service::VFSService;
use crate::io::uri;
use crate::io::PosixVFSService;
use crate::storage;
use crate::Result;

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (nfields: u32))]
struct FragmentFileOffsets {
    #[br(count(nfields))]
    fixed_sizes: Vec<u64>,

    #[br(count(nfields))]
    var_sizes: Vec<u64>,

    #[br(count(nfields))]
    validity_sizes: Vec<u64>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (nfields: u32))]
struct FragmentTileOffsets {
    rtree: u64,

    #[br(count(nfields))]
    fixed_offsets: Vec<u64>,

    #[br(count(nfields))]
    var_offsets: Vec<u64>,

    #[br(count(nfields))]
    var_sizes: Vec<u64>,

    #[br(count(nfields))]
    validity_offsets: Vec<u64>,

    #[br(count(nfields))]
    min_offsets: Vec<u64>,

    #[br(count(nfields))]
    max_offsets: Vec<u64>,

    #[br(count(nfields))]
    sum_offsets: Vec<u64>,

    #[br(count(nfields))]
    null_count_offsets: Vec<u64>,

    frag_meta_offset: u64,

    processed_conditions_offset: u64,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (nfields: u32))]
struct FragmentFooter {
    version: u32,

    array_schema_size: u64,

    #[br(count(name_size))]
    #[br(map = |v: Vec<u8>| String::from_utf8(v).unwrap())]
    #[bw(map = |n: &String| n.as_bytes().to_vec())]
    array_scheam: String,

    fragment_type: u8,

    null_non_empty_domain: u8,

    #[br(if(null_non_empty_domain == 0, Vec::new()))]
    #[br(count(4))]
    non_empty_domain: Vec<f64>,

    sparse_tile_num: u64,
    last_tile_cell_num: u64,
    has_timestamps: u8,
    has_delete_meta: u8,

    #[br(args(nfields))]
    file_offsets: FragmentFileOffsets,

    #[br(args(nfields))]
    tile_offsets: FragmentTileOffsets,
}

struct FragmentMetadata {}

pub fn read_fragment_metadata(
    uri: &uri::URI,
    u32: nfields,
) -> Result<FragmentMetadata> {
    let vfs = PosixVFSService::default();

    let file_size = vfs.file_size(uri)?;

    let size = 8;
    let data = vfs.file_read_vec(uri, size, offset)?;
    let mut reader = Cursor::new(data);

    let fmd = FragmentMetadata {};
    Ok(fmd)
}
