// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::fs;
use std::io::Read;
use std::path::PathBuf;

pub fn read_file(path: String) -> Vec<u8> {
    let mut f = fs::File::open(&path).unwrap_or_else(|err| {
        panic!("File not found: {} because {}", path, err)
    });
    let metadata = fs::metadata(&path).unwrap_or_else(|err| {
        panic!("File not found: {} because {}", path, err)
    });

    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).unwrap_or_else(|err| {
        panic!("Error reading file: {} because {}", path, err)
    });

    buffer
}

pub fn read_test_file(file_name: String) -> Vec<u8> {
    let mut path = PathBuf::new();
    path.push(env!("CARGO_MANIFEST_DIR"));
    path.push("..");
    path.push(file_name);
    read_file(path.as_path().to_string_lossy().to_string())
}
