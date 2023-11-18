// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use binrw::binrw;

use crate::filters::FilterType;

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
enum FilterConfig {
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
struct Filter {
    #[br(map = |ftype: u8| ftype.into())]
    #[bw(map = |ftype: &FilterType| *ftype as u8)]
    filter_type: FilterType,
    metadata_len: u32,

    #[br(args { filter_type })]
    config: FilterConfig,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
struct FilterPipeline {
    max_chunk_size: u32,
    num_filters: u32,

    #[br(count(num_filters))]
    filters: Vec<Filter>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
struct GenericTileHeader {
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
    use binrw::io::Cursor;
    use binrw::BinRead;
    use util::read_test_file;

    #[test]
    fn basic_read() {
        let data = read_test_file("resources/schema/schema_1".to_string());
        let mut reader = Cursor::new(data);
        let header = GenericTileHeader::read(&mut reader).unwrap();
        println!("{:?}", header);

        let pipeline = FilterPipeline::read(&mut reader).unwrap();
        println!("{:?}", pipeline);
    }
}
