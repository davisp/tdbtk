// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::collections::HashMap;
use std::convert::TryFrom;

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
    extent: Vec<u8>,
}

impl TryFrom<&storage::schema::Dimension> for Dimension {
    type Error = anyhow::Error;
    fn try_from(
        storage: &storage::schema::Dimension,
    ) -> Result<Dimension, Self::Error> {
        Ok(Dimension {
            name: String::from_utf8(storage.name.clone())?,
            data_type: storage.data_type,
            cell_val_num: storage.cell_val_num,
            filters: <_>::try_from(&storage.coords_filters)?,
            range: storage.range.clone(),
            extent: storage.tile_extent.clone(),
        })
    }
}

pub struct Domain {
    dimensions: Vec<Dimension>,
}

impl TryFrom<&storage::schema::Domain> for Domain {
    type Error = anyhow::Error;
    fn try_from(
        storage: &storage::schema::Domain,
    ) -> Result<Domain, Self::Error> {
        let mut dimensions = Vec::new();
        for dim in storage.dimensions.iter() {
            dimensions.push(Dimension::try_from(dim)?);
        }
        Ok(Domain { dimensions })
    }
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

impl TryFrom<&storage::schema::Attribute> for Attribute {
    type Error = anyhow::Error;
    fn try_from(
        storage: &storage::schema::Attribute,
    ) -> Result<Attribute, Self::Error> {
        Ok(Attribute {
            name: storage.name.clone(),
            data_type: storage.data_type,
            cell_val_num: storage.cell_val_num,
            filters: <_>::try_from(&storage.filters)?,
            fill_value: storage.fill_value.clone(),
            nullable: storage.nullable != 0,
            fill_value_validity: storage.fill_value_validity != 0,
            data_order: storage.data_order,
            enumeration_name: String::from_utf8(
                storage.enumeration_name.clone(),
            )?,
        })
    }
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

impl TryFrom<&storage::schema::DimensionLabel> for DimensionLabel {
    type Error = anyhow::Error;
    fn try_from(
        storage: &storage::schema::DimensionLabel,
    ) -> Result<DimensionLabel, Self::Error> {
        Ok(DimensionLabel {
            dimension_idx: storage.dimension_id,
            name: String::from_utf8(storage.name.clone())?,
            relative_uri: storage.relative_uri != 0,
            uri: uri::URI::from_string(&String::from_utf8(
                storage.uri.clone(),
            )?)?,
            attribute_name: String::from_utf8(storage.attribute_name.clone())?,
            data_order: storage.data_order,
            data_type: storage.data_type,
            cell_val_num: storage.cell_val_num,
            is_external: storage.is_external != 0,
        })
    }
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

impl TryFrom<storage::schema::ArraySchema> for ArraySchema {
    type Error = anyhow::Error;

    fn try_from(
        storage: storage::schema::ArraySchema,
    ) -> Result<ArraySchema, Self::Error> {
        let mut attrs = Vec::new();
        for attr in storage.attributes.iter() {
            attrs.push(Attribute::try_from(attr)?);
        }
        let mut dim_labels = Vec::new();
        for dl in storage.dimension_labels.iter() {
            dim_labels.push(DimensionLabel::try_from(dl)?);
        }
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
            domain: Domain::try_from(&storage.domain)?,
            attributes: attrs,
            dimension_labels: dim_labels,
            enumerations: storage.enumeration_map,
        })
    }
}
