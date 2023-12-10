// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::Result;

use crate::io::uri;
use crate::io::FSEntry;

pub struct WalkOptions {
    min_depth: usize,
    max_depth: usize,
    follow_links: bool,
    follow_root_links: bool,
    sort_filenames: bool,
}

impl Default for WalkOptions {
    fn default() -> Self {
        WalkOptions {
            min_depth: 0,
            max_depth: usize::MAX,
            follow_links: false,
            follow_root_links: true,
            sort_filenames: false,
        }
    }
}

// PJD: Are public fields more idiomatic that setter/getter functions?
impl WalkOptions {
    pub fn min_depth(&self) -> usize {
        self.min_depth
    }

    pub fn set_min_depth(mut self, min_depth: usize) -> Self {
        self.min_depth = min_depth;
        self
    }

    pub fn max_depth(&self) -> usize {
        self.max_depth
    }

    pub fn set_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
    }

    pub fn follow_links(&self) -> bool {
        self.follow_links
    }

    pub fn set_follow_links(mut self, follow_links: bool) -> Self {
        self.follow_links = follow_links;
        self
    }

    pub fn follow_root_links(&self) -> bool {
        self.follow_root_links
    }

    pub fn set_follow_root_links(mut self, follow_root_links: bool) -> Self {
        self.follow_root_links = follow_root_links;
        self
    }

    pub fn sort_filenames(&self) -> bool {
        self.sort_filenames
    }

    pub fn set_sort_filenames(mut self, sort_filenames: bool) -> Self {
        self.sort_filenames = sort_filenames;
        self
    }
}

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
    fn walk(
        &self,
        uri: &uri::URI,
        callback: &mut dyn FnMut(&FSEntry) -> Result<bool>,
    ) -> Result<()>;

    fn walk_with_options(
        &self,
        uri: &uri::URI,
        options: &WalkOptions,
        callback: &mut dyn FnMut(&FSEntry) -> Result<bool>,
    ) -> Result<()>;
}
