// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use binrw::io::Cursor;
use binrw::BinRead;

use crate::storage;
use crate::Result;

pub fn decompress(
    do_decompress: &dyn Fn(&[u8], &mut [u8]) -> Result<()>,
    input: &mut storage::Chunk,
    output: &mut storage::Chunk,
) -> Result<()> {
    let mut reader = Cursor::new(&(input.metadata));
    let comp_info = storage::CompressionChunks::read(&mut reader)?;

    // Create our metadata buffer
    let total_metadata_size: usize = comp_info
        .metadata_parts
        .iter()
        .map(|chunk| chunk.uncompressed_size as usize)
        .sum();
    output.metadata.resize(total_metadata_size, 0);

    // Create our data buffer
    let total_data_size: usize = comp_info
        .data_parts
        .iter()
        .map(|chunk| chunk.uncompressed_size as usize)
        .sum();
    output.data.resize(total_data_size, 0);

    let decompress = |chunk: &storage::CompressionChunkInfo,
                      output: &mut [u8],
                      input_offset: &mut usize,
                      output_offset: &mut usize|
     -> Result<()> {
        let input_end = *input_offset + chunk.compressed_size as usize;
        let output_end = *output_offset + chunk.uncompressed_size as usize;
        do_decompress(
            &(input.data)[*input_offset..input_end],
            &mut output[*output_offset..output_end],
        )?;
        *input_offset += chunk.compressed_size as usize;
        *output_offset += chunk.uncompressed_size as usize;
        Ok(())
    };

    // Track where we are in the input data buffer
    let mut input_offset = 0;

    // Track where we are in the output metadata buffer
    let mut output_offset = 0;

    // Decompress metadata chunks
    for chunk in comp_info.metadata_parts {
        decompress(
            &chunk,
            &mut output.metadata,
            &mut input_offset,
            &mut output_offset,
        )?
    }

    // Track where we are in the output data buffer
    let mut output_offset = 0;

    // Decompress data chunks
    for chunk in comp_info.data_parts {
        decompress(
            &chunk,
            &mut output.data,
            &mut input_offset,
            &mut output_offset,
        )?
    }

    Ok(())
}
