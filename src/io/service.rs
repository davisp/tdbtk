// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::Result;

use crate::io::uri;
use crate::io::FSEntry;

pub trait VFSService {
    // Buckets
    fn bucket_supported(&self) -> Result<bool>;
    fn bucket_exists(&self, uri: &uri::URI) -> Result<bool>;
    fn bucket_is_empty(&self, uri: &uri::URI) -> Result<bool>;
    fn bucket_create(&self, uri: &uri::URI) -> Result<()>;
    fn bucket_remove(&self, uri: &uri::URI) -> Result<bool>;
    fn bucket_clear(&self, uri: &uri::URI) -> Result<()>;

    // Directories
    fn dir_exists(&self, uri: &uri::URI) -> Result<bool>;
    fn dir_size(&self, uri: &uri::URI) -> Result<u64>;
    fn dir_create(&self, uri: &uri::URI) -> Result<()>;
    fn dir_move(&self, src_uri: &uri::URI, dst_uri: &uri::URI) -> Result<()>;
    fn dir_copy(&self, src_uri: &uri::URI, dst_uri: &uri::URI) -> Result<()>;
    fn dir_remove(&self, uri: &uri::URI) -> Result<()>;

    // Files
    fn file_exists(&self, uri: &uri::URI) -> Result<bool>;
    fn file_size(&self, uri: &uri::URI) -> Result<u64>;
    fn file_create(&self, uri: &uri::URI) -> Result<()>;
    fn file_read(
        &self,
        uri: &uri::URI,
        nbytes: u64,
        offset: u64,
        buffer: &mut [u8],
    ) -> Result<()>;
    fn file_read_vec(
        &self,
        uri: &uri::URI,
        nbytes: u64,
        offset: u64,
    ) -> Result<Vec<u8>>;
    fn file_write(
        &self,
        uri: &uri::URI,
        offset: u64,
        buffer: &[u8],
    ) -> Result<()>;
    fn file_move(&self, src_uri: &uri::URI, dst_uri: &uri::URI) -> Result<()>;
    fn file_copy(&self, src_uri: &uri::URI, dst_uri: &uri::URI) -> Result<()>;
    fn file_sync(&self, uri: &uri::URI) -> Result<()>;
    fn file_remove(&self, uri: &uri::URI) -> Result<()>;

    fn ls(&self, uri: &uri::URI) -> Result<Vec<FSEntry>>;
    fn walk_files<F>(&self, uri: &uri::URI, f: F) -> Result<()>
    where
        F: FnMut(&FSEntry) -> Result<bool>;
}
