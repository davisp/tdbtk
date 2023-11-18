use binrw::binrw;

use crate::filters::FilterType;

#[binrw]
enum FilterConfig {
    None,
    Compression {
        compressor_type: u8,
        compression_level: i32,
        #[br(if(compressor_type == FilterType::Delta as u8
            || compressor_type == FilterType::DoubleDelta as u8))]
        reinterpret_type: u8,
    },
    BitWidthReduction {
        max_window_size: u32,
    },
    PositiveDelta {
        max_window_size: u32,
    },
    FloatScaling {
        scale: f64,
        offset: f64,
        byte_width: u64,
    },
    WebP {
        quality: f32,
        format: u8,
        lossless: u8,
        y_extent: u16,
        x_extent: u16,
    },
}

#[binrw]
struct FilterHeader {
    filter_type: u8,
    metadata_len: u32,
}

#[binrw]
struct FilterPipeline {
    max_chunk_size: u32,
    num_filters: u32,
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
    filter_pipeline_size: u64,
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
    }
}
