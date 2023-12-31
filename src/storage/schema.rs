// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::collections::HashMap;

use anyhow::anyhow;
use binrw::io::Cursor;
use binrw::{binrw, BinRead, BinResult, BinWrite, Error, VecArgs};

use crate::array::{ArrayType, DataOrder, Layout};
use crate::datatype::DataType;
use crate::io::uri;
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
    pub(crate) name: Vec<u8>,

    #[br(if(version >= 5, dtype))]
    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    pub(crate) data_type: DataType,

    #[br(if(version >= 5, cell_val_size(dtype)))]
    pub(crate) cell_val_num: u32,

    #[br(if(version >= 5, coords_filters))]
    pub(crate) coords_filters: storage::FilterList,

    #[br(if(version >= 5, 2 * data_type.size() as u64))]
    domain_size: u64,

    #[br(count = domain_size)]
    pub(crate) range: Vec<u8>,

    null_tile_extent: u8,

    #[br(if(null_tile_extent == 0))]
    #[br(count = data_type.size())]
    pub(crate) tile_extent: Vec<u8>,
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
    pub(crate) dimensions: Vec<Dimension>,
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
    pub(crate) name: String,

    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    pub(crate) data_type: DataType,

    pub(crate) cell_val_num: u32,

    #[br(args(version))]
    pub(crate) filters: storage::FilterList,

    #[br(if(version >= 6, 0))]
    fill_value_size: u64,

    #[br(count = fill_value_size)]
    pub(crate) fill_value: Vec<u8>,

    #[br(if(version >= 7, 0))]
    pub(crate) nullable: u8,

    #[br(if(version >= 7, 0))]
    pub(crate) fill_value_validity: u8,

    #[br(if(version >= 17, DataOrder::Unordered))]
    #[br(map = |order: u8| order.into())]
    #[bw(map = |order: &DataOrder| *order as u8)]
    #[brw(assert(!matches!(data_order, DataOrder::Invalid)))]
    pub(crate) data_order: DataOrder,

    #[br(if(version >= 20, 0))]
    enmr_name_length: u32,

    #[br(count = enmr_name_length)]
    pub(crate) enumeration_name: Vec<u8>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct DimensionLabel {
    pub(crate) dimension_id: u32,

    name_len: u32,

    #[br(count = name_len)]
    pub(crate) name: Vec<u8>,

    pub(crate) relative_uri: u8,

    uri_size: u64,

    #[br(count = uri_size)]
    pub(crate) uri: Vec<u8>,

    attribute_name_len: u32,

    #[br(count = attribute_name_len)]
    pub(crate) attribute_name: Vec<u8>,

    #[br(map = |order: u8| order.into())]
    #[bw(map = |order: &DataOrder| *order as u8)]
    #[brw(assert(!matches!(data_order, DataOrder::Invalid)))]
    pub(crate) data_order: DataOrder,

    #[br(map = |dtype: u8| dtype.into())]
    #[bw(map = |dtype: &DataType| *dtype as u8)]
    #[brw(assert(!matches!(data_type, DataType::Invalid)))]
    pub(crate) data_type: DataType,

    pub(crate) cell_val_num: u32,

    pub(crate) is_external: u8,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
pub struct ArraySchema {
    #[br(assert(version <= storage::CURRENT_FORMAT_VERSION,
        "Invalid version {} is newer than library version {}",
            version, storage::CURRENT_FORMAT_VERSION))]
    pub(crate) version: u32,

    #[br(if(version >= 5, 0))]
    pub(crate) allows_dups: u8,

    #[br(map = |atype: u8| atype.into())]
    #[bw(map = |atype: &ArrayType| *atype as u8)]
    #[brw(assert(!matches!(array_type, ArrayType::Invalid)))]
    pub(crate) array_type: ArrayType,

    #[br(map = |layout: u8| layout.into())]
    #[bw(map = |layout: &Layout| *layout as u8)]
    #[brw(assert(!matches!(tile_order, Layout::Invalid)))]
    pub(crate) tile_order: Layout,

    #[br(map = |layout: u8| layout.into())]
    #[bw(map = |layout: &Layout| *layout as u8)]
    #[brw(assert(!matches!(cell_order, Layout::Invalid)))]
    pub(crate) cell_order: Layout,

    pub(crate) capacity: u64,

    #[br(args(version))]
    pub(crate) coords_filters: storage::FilterList,

    #[br(args(version))]
    pub(crate) cell_var_filters: storage::FilterList,

    #[br(args(version))]
    #[br(if(version >= 7, storage::FilterList::default()))]
    pub(crate) cell_validity_filters: storage::FilterList,

    #[br(args { version, coords_filters: coords_filters.clone() })]
    pub(crate) domain: Domain,

    pub(crate) num_attributes: u32,

    #[br(count = num_attributes, args {inner: (
        version,
    )})]
    pub(crate) attributes: Vec<Attribute>,

    #[br(if(version >= 18, 0))]
    pub(crate) num_dimension_labels: u32,

    #[br(count = num_dimension_labels)]
    pub(crate) dimension_labels: Vec<DimensionLabel>,

    #[br(parse_with = enumeration_name_map_parser)]
    #[bw(write_with = enumeration_name_map_writer)]
    pub(crate) enumeration_map: HashMap<String, String>,
}

impl ArraySchema {
    pub fn load(uri: &uri::URI) -> Result<ArraySchema> {
        let data = storage::read_generic_tile(uri, 0)?;
        let mut reader = Cursor::new(data);
        let s = ArraySchema::read(&mut reader).map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error reading schema data from {}", uri.to_string())
                .context(context)
        })?;

        Ok(s)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_read() -> Result<()> {
        let _ = ArraySchema::load(&uri::URI::from_string(
            "resources/schema/schema_1",
        )?)?;
        Ok(())
    }

    #[test]
    fn test_read() -> Result<()> {
        let uri = uri::URI::from_string("/Users/davisp/github/tiledb/unit-test-arrays/v2_9_1/SPARSE_v2_9_1_UINT16_DATETIME_US/__schema/__1653499966512_1653499966512_8135e35bf7c9483892957c6e0bcbd86a")?;
        let _ = ArraySchema::load(&uri)?;

        Ok(())
    }
}
