// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::collections::HashMap;

use anyhow::anyhow;
use binrw::io::Cursor;
use binrw::{binrw, BinRead, BinResult, BinWrite, Error, VecArgs};

use crate::array::{ArrayType, DataOrder, Layout};
use crate::datatype::DataType;
use crate::filters;
use crate::io::service::VFSService;
use crate::io::uri;
use crate::io::PosixVFSService;
use crate::storage;
use crate::Result;

pub const CELL_VAR_SIZE: u32 = u32::MAX;

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (version: u32, dtype: DataType, coords_filters: storage::FilterList))]
pub struct Dimension {
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

    #[br(if(version >= 5, coords_filters))]
    coords_filters: storage::FilterList,

    #[br(if(version >= 5, 2 * data_type.size() as u64))]
    domain_size: u64,

    #[br(count = domain_size)]
    range: Vec<u8>,

    null_tile_extent: u8,

    #[br(if(null_tile_extent == 0))]
    #[br(count = data_type.size())]
    tile_extent: Vec<u8>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import { version: u32, coords_filters: storage::FilterList })]
pub struct Domain {
    #[br(if(version < 5, DataType::Int32))]
    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    data_type: DataType,

    num_dimensions: u32,

    #[br(count = num_dimensions, args {inner: (
        version,
        data_type,
        coords_filters
    )})]
    dimensions: Vec<Dimension>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import ( version: u32 ))]
pub struct Attribute {
    name_size: u32,

    #[br(count(name_size))]
    #[br(map = |v: Vec<u8>| String::from_utf8(v).unwrap())]
    #[bw(map = |n: &String| n.as_bytes().to_vec())]
    name: String,

    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    data_type: DataType,

    cell_val_num: u32,

    #[br(args(version))]
    filters: storage::FilterList,

    #[br(if(version >= 6, 0))]
    fill_value_size: u64,

    #[br(count = fill_value_size)]
    fill_value: Vec<u8>,

    #[br(if(version >= 7, 0))]
    nullable: u8,

    #[br(if(version >= 7, 0))]
    fill_value_validity: u8,

    #[br(if(version >= 17, DataOrder::Unordered))]
    #[br(map = |order: u8| order.into())]
    #[bw(map = |order: &DataOrder| *order as u8)]
    #[brw(assert(!matches!(data_order, DataOrder::Invalid)))]
    data_order: DataOrder,

    #[br(if(version >= 20, 0))]
    enmr_name_length: u32,

    #[br(count = enmr_name_length)]
    enumeration_name: Vec<u8>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct DimensionLabel {
    dimension_id: u32,

    name_len: u32,

    #[br(count = name_len)]
    name: Vec<u8>,

    relative_uri: u8,

    uri_size: u64,

    #[br(count = uri_size)]
    uri: Vec<u8>,

    attribute_name_len: u32,

    #[br(count = attribute_name_len)]
    attribute_name: Vec<u8>,

    #[br(map = |order: u8| order.into())]
    #[bw(map = |order: &DataOrder| *order as u8)]
    #[brw(assert(!matches!(data_order, DataOrder::Invalid)))]
    data_order: DataOrder,

    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    data_type: DataType,

    cell_val_num: u32,

    is_external: u8,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct ArraySchema {
    #[br(dbg)]
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

    #[br(dbg)]
    capacity: u64,

    #[br(args(version))]
    coords_filters: storage::FilterList,

    #[br(args(version))]
    cell_var_filters: storage::FilterList,

    #[br(args(version))]
    #[br(if(version >= 7, storage::FilterList::default()))]
    cell_validity_filters: storage::FilterList,

    #[br(args { version, coords_filters: coords_filters.clone() })]
    domain: Domain,

    num_attributes: u32,

    #[br(count = num_attributes, args {inner: (
        version,
    )})]
    attributes: Vec<Attribute>,

    #[br(if(version >= 18, 0))]
    num_dimension_labels: u32,

    #[br(count = num_dimension_labels)]
    dimension_labels: Vec<DimensionLabel>,

    #[br(parse_with = enumeration_name_map_parser)]
    #[bw(write_with = enumeration_name_map_writer)]
    enumeration_map: HashMap<String, String>,
}

fn cell_val_size(dtype: DataType) -> u32 {
    if dtype.is_string_type() {
        CELL_VAR_SIZE
    } else {
        1
    }
}

#[binrw::parser(reader, endian)]
fn enumeration_name_map_parser(
    version: u32,
) -> BinResult<HashMap<String, String>> {
    let mut map = HashMap::new();

    if version < 20 {
        return Ok(map);
    }

    let num_entries = <u32>::read_options(reader, endian, ())?;

    for _ in 0..num_entries {
        let name_size = <u32>::read_options(reader, endian, ())?;
        let name_vec = <Vec<u8>>::read_options(
            reader,
            endian,
            VecArgs {
                count: name_size as usize,
                inner: <_>::default(),
            },
        )?;
        let name =
            String::from_utf8(name_vec).map_err(|err| Error::Custom {
                pos: 0,
                err: Box::new(err),
            })?;

        let path_size = <u32>::read_options(reader, endian, ())?;
        let path_vec = <Vec<u8>>::read_options(
            reader,
            endian,
            VecArgs {
                count: path_size as usize,
                inner: <_>::default(),
            },
        )?;
        let path =
            String::from_utf8(path_vec).map_err(|err| Error::Custom {
                pos: 0,
                err: Box::new(err),
            })?;

        map.insert(name, path);
    }

    Ok(map)
}

#[binrw::writer(writer, endian)]
fn enumeration_name_map_writer(map: &HashMap<String, String>) -> BinResult<()> {
    <u32>::write_options(&(map.len() as u32), writer, endian, ())?;
    for (k, v) in map {
        <u32>::write_options(&(k.len() as u32), writer, endian, ())?;
        <Vec<u8>>::write_options(&(k.as_bytes().to_vec()), writer, endian, ())?;

        <u32>::write_options(&(v.len() as u32), writer, endian, ())?;
        <Vec<u8>>::write_options(&(v.as_bytes().to_vec()), writer, endian, ())?;
    }

    Ok(())
}

pub fn read(uri: &uri::URI) -> Result<ArraySchema> {
    let vfs = PosixVFSService::default();

    let schema_data = vfs.file_read_vec(uri, u64::MAX, 0)?;
    let mut reader = Cursor::new(schema_data);

    let header = storage::GenericTileHeader::read(&mut reader)?;
    let pipeline =
        storage::FilterList::read_args(&mut reader, (header.version,))?;
    let chain = filters::FilterChain::from_list(&pipeline);
    let mut chunks = storage::ChunkedData::read(&mut reader)?;

    let data = chain.unfilter_chunks(&mut chunks).map_err(|err| {
        let context = format!("{:?}", err);
        anyhow!("Error unfiltering schema data from {}", uri.to_string())
            .context(context)
    })?;

    let mut reader = Cursor::new(data);
    let s = ArraySchema::read(&mut reader).map_err(|err| {
        let context = format!("{:?}", err);
        anyhow!("Error reading schema data from {}", uri.to_string())
            .context(context)
    })?;

    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_read() -> Result<()> {
        let _ = read(&uri::URI::from_string("resources/schema/schema_1")?)?;
        Ok(())
    }

    #[test]
    fn test_read() -> Result<()> {
        let uri = uri::URI::from_string("/Users/davisp/github/tiledb/unit-test-arrays/v2_9_1/SPARSE_v2_9_1_UINT16_DATETIME_US/__schema/__1653499966512_1653499966512_8135e35bf7c9483892957c6e0bcbd86a")?;
        let _ = read(&uri)?;

        Ok(())
    }
}
