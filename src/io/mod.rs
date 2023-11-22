// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

pub mod posix;
pub mod service;
pub mod uri;

pub use self::posix::PosixVFSService;
pub use self::service::VFSService;

#[derive(Clone)]
pub enum FSEntryType {
    Dir,
    File,
    Unknown,
}

#[derive(Clone)]
pub struct FSEntry {
    uri: uri::URI,
    entry_type: FSEntryType,
    size: u64,
}

impl FSEntry {
    pub fn new(uri: uri::URI, entry_type: FSEntryType, size: u64) -> Self {
        FSEntry {
            uri,
            entry_type,
            size,
        }
    }

    pub fn uri(&self) -> uri::URI {
        self.uri.clone()
    }

    pub fn entry_type(&self) -> FSEntryType {
        self.entry_type.clone()
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}
