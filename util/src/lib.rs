// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::fs;
use std::path::PathBuf;

use positioned_io::ReadAt;

pub fn read_file(path: String, nbytes: u64, offset: u64) -> Vec<u8> {
    let f = fs::File::open(&path).unwrap_or_else(|err| {
        panic!("File not found: {} because {}", path, err)
    });

    let to_read = if nbytes == u64::MAX {
        let metadata = fs::metadata(&path).unwrap_or_else(|err| {
            panic!("File not found: {} because {}", path, err)
        });
        metadata.len()
    } else {
        nbytes
    };

    let mut buffer = vec![0; to_read as usize];
    f.read_at(offset, &mut buffer).unwrap_or_else(|err| {
        panic!("Error reading file: {} because {}", path, err)
    });

    buffer
}

pub fn test_file_name(file_name: String) -> String {
    let mut path = PathBuf::new();
    path.push(env!("CARGO_MANIFEST_DIR"));
    path.push("..");
    path.push(file_name);
    path.as_path().to_string_lossy().to_string()
}

pub fn read_test_file(file_name: String) -> Vec<u8> {
    read_file(test_file_name(file_name), u64::MAX, 0)
}

pub fn read_test_file_at(
    file_name: String,
    nbytes: u64,
    offset: u64,
) -> Vec<u8> {
    read_file(test_file_name(file_name), nbytes, offset)
}
