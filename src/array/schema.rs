// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::anyhow;

use crate::array::{ArrayType, DataOrder, Layout};
use crate::datatype::DataType;
use crate::filters::FilterChain;
use crate::io::uri;
use crate::storage;

pub struct Dimension {
    name: String,
    data_type: DataType,
    cell_val_num: u32,
    filters: Box<FilterChain>,
    range: Vec<u8>,
    tile_extent: Vec<u8>,
}

pub struct Domain {
    dimensions: Vec<Dimension>,
}

pub struct Attribute {
    name: String,
    data_type: DataType,
    cell_val_num: u32,
    filters: Box<FilterChain>,
    fill_value: Vec<u8>,
    nullable: bool,
    fill_value_validity: bool,
    data_order: DataOrder,
    enumeration_name: String,
}

pub struct DimensionLabel {
    dimension_idx: u32,
    name: String,
    relative_uri: bool,
    uri: uri::URI,
    attribute_name: String,
    data_order: DataOrder,
    data_type: DataType,
    cell_val_num: u32,
    is_external: bool,
}

pub struct ArraySchema {
    version: u32,
    allows_dups: bool,
    array_type: ArrayType,
    tile_order: Layout,
    cell_order: Layout,
    capacity: u64,
    cell_var_filters: Box<FilterChain>,
    cell_validity_filters: Box<FilterChain>,
    domain: Domain,
    attributes: Vec<Attribute>,
    dimension_labels: Vec<DimensionLabel>,
    enumerations: HashMap<String, String>,
}

impl TryFrom<storage::ArraySchema> for ArraySchema {
    type Error = anyhow::Error;

    fn try_from(
        storage: storage::ArraySchema,
    ) -> Result<ArraySchema, Self::Error> {
        Ok(ArraySchema {
            version: storage.version,
            allows_dups: storage.allows_dups != 0,
            array_type: storage.array_type,
            tile_order: storage.tile_order,
            cell_order: storage.cell_order,
            capacity: storage.capacity,
            cell_var_filters: <_>::try_from(&storage.cell_var_filters)?,
            cell_validity_filters: <_>::try_from(
                &storage.cell_validity_filters,
            )?,
            domain: Domain::try_from(storage.domain),
            attributes: storage.attributes.map(|a| Attribute::try_from(a)),
        })
    }
}
