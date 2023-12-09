// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use binrw::binrw;

use crate::filters::FilterType;

fn is_compression_filter(ftype: FilterType) -> bool {
    matches!(
        ftype as FilterType,
        FilterType::GZip
            | FilterType::Zstd
            | FilterType::LZ4
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

fn has_reinterpret_type(version: u32, filter_type: FilterType) -> bool {
    if version >= 19 && matches!(filter_type, FilterType::Delta) {
        return true;
    }

    if version >= 20 && matches!(filter_type, FilterType::DoubleDelta) {
        return true;
    }

    false
}

#[derive(Clone, Debug, Default)]
#[binrw]
#[brw(little)]
#[br(import { version: u32, filter_type: FilterType })]
pub enum FilterConfig {
    #[br(pre_assert(is_compression_filter(filter_type)))]
    Compression {
        #[br(map = |ftype: u8| ftype.into())]
        #[bw(map = |ftype: &FilterType| *ftype as u8)]
        compressor_type: FilterType,

        compression_level: i32,

        #[br(if(has_reinterpret_type(version, filter_type)))]
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

#[derive(Clone, Debug, Default)]
#[binrw]
#[brw(little)]
#[br(import ( version: u32 ))]
pub struct Filter {
    #[br(map = |ftype: u8| ftype.into())]
    #[bw(map = |ftype: &FilterType| *ftype as u8)]
    #[brw(assert(!matches!(filter_type, FilterType::Invalid)))]
    filter_type: FilterType,

    metadata_len: u32,

    #[br(args { version, filter_type })]
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

#[derive(Clone, Debug, Default)]
#[binrw]
#[brw(little)]
#[br(import ( version: u32 ))]
pub struct FilterList {
    max_chunk_size: u32,

    num_filters: u32,

    #[br(count(num_filters))]
    #[br(args {inner: (version,)})]
    filters: Vec<Filter>,
}

impl FilterList {
    pub fn filters(&self) -> &[Filter] {
        &self.filters
    }
}
