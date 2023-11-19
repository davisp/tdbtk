// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use binrw::binrw;

use crate::array::{ArrayType, Layout};
use crate::datatype::DataType;
use crate::storage;

pub const CELL_VAR_SIZE: u32 = u32::MAX;

fn cell_val_size(dtype: DataType) -> u32 {
    if dtype.is_string_type() {
        CELL_VAR_SIZE
    } else {
        1
    }
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import { version: u32, dtype: DataType })]
struct Dimension {
    name_size: u32,

    #[br(count(name_size))]
    name: Vec<u8>,

    #[br(if(version >= 5, dtype))]
    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    data_type: DataType,

    #[br(if(version >= 5, cell_val_size(dtype)))]
    cell_val_num: u32,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import { version: u32 })]
struct Domain {
    #[br(if(version < 5, DataType::Int32))]
    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    data_type: DataType,

    num_dimensions: u32,

    #[br(count(num_dimensions))]
    #[br(args { version, data_type })]
    dimensions: Vec<Dimension>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
struct ArraySchema {
    #[br(assert(version <= storage::CURRENT_FORMAT_VERSION,
        "Invalid version {} is newer than library version {}",
            version, storage::CURRENT_FORMAT_VERSION))]
    version: u32,

    #[br(if(version >= 5, 0))]
    allows_dups: u8,

    #[br(map = |atype: u8| atype.into())]
    #[bw(map = |atype: &ArrayType| *atype as u8)]
    #[brw(assert(!matches!(array_type, ArrayType::Invalid)))]
    array_type: ArrayType,

    #[br(map = |layout: u8| layout.into())]
    #[bw(map = |layout: &Layout| *layout as u8)]
    #[brw(assert(!matches!(tile_order, Layout::Invalid)))]
    tile_order: Layout,

    #[br(map = |layout: u8| layout.into())]
    #[bw(map = |layout: &Layout| *layout as u8)]
    #[brw(assert(!matches!(cell_order, Layout::Invalid)))]
    cell_order: Layout,

    capacity: u64,

    coords_filters: storage::FilterList,
    cell_var_filters: storage::FilterList,
    cell_validity_filters: storage::FilterList,
}
