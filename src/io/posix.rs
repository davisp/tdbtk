// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::fs;
use std::io;

use anyhow::{anyhow, Result};
use positioned_io::{ReadAt, WriteAt};

use crate::io::service::VFSService;
use crate::io::uri;
use crate::io::{FSEntry, FSEntryType};

#[derive(Default)]
pub struct PosixVFSService {}

impl PosixVFSService {
    fn open_file(&self, uri: &uri::URI) -> Result<fs::File> {
        fs::File::open(uri.path()).map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error opening {}", uri.to_string()).context(context)
        })
    }
}

impl VFSService for PosixVFSService {
    fn bucket_supported(&self) -> Result<bool> {
        Ok(false)
    }

    fn bucket_exists(&self, _uri: &uri::URI) -> Result<bool> {
        Err(anyhow!("Local file systems do not support buckets."))
    }

    fn bucket_is_empty(&self, _uri: &uri::URI) -> Result<bool> {
        Err(anyhow!("Local file systems do not support buckets."))
    }

    fn bucket_create(&self, _uri: &uri::URI) -> Result<()> {
        Err(anyhow!("Local file systems do not support buckets."))
    }

    fn bucket_remove(&self, _uri: &uri::URI) -> Result<bool> {
        Err(anyhow!("Local file systems do not support buckets."))
    }

    fn bucket_clear(&self, _uri: &uri::URI) -> Result<()> {
        Err(anyhow!("Local file systems do not support buckets."))
    }

    fn dir_exists(&self, uri: &uri::URI) -> Result<bool> {
        let res = fs::metadata(uri.path());
        let md = match res {
            Ok(md) => md,
            Err(err) => {
                if err.kind() == io::ErrorKind::NotFound {
                    return Ok(false);
                } else {
                    return Err(err.into());
                }
            }
        };

        Ok(md.file_type().is_dir())
    }

    fn dir_size(&self, uri: &uri::URI) -> Result<u64> {
        let mut size = 0;
        self.walk_files(uri, |entry: &FSEntry| {
            size += entry.size();
            Ok(true)
        })?;
        Ok(size)
    }

    fn dir_create(&self, uri: &uri::URI) -> Result<()> {
        let builder = fs::DirBuilder::new();
        Ok(builder.create(uri.path())?)
    }

    fn dir_move(&self, _src_uri: &uri::URI, _dst_uri: &uri::URI) -> Result<()> {
        unimplemented!("Not implemented.")
    }

    fn dir_copy(&self, _src_uri: &uri::URI, _dst_uri: &uri::URI) -> Result<()> {
        unimplemented!("Not implemented.")
    }

    fn dir_remove(&self, uri: &uri::URI) -> Result<()> {
        Ok(fs::remove_dir_all(uri.path())?)
    }

    fn file_exists(&self, uri: &uri::URI) -> Result<bool> {
        let res = fs::metadata(uri.path());
        let md = match res {
            Ok(md) => md,
            Err(err) => {
                if err.kind() == io::ErrorKind::NotFound {
                    return Ok(false);
                } else {
                    return Err(err.into());
                }
            }
        };

        Ok(md.file_type().is_file())
    }

    fn file_size(&self, uri: &uri::URI) -> Result<u64> {
        let md = fs::metadata(uri.path())?;
        if !md.is_file() {
            let ctx = format!("URI: {}", uri.to_string());
            return Err(anyhow!("URI is not a file.").context(ctx));
        }

        Ok(md.len())
    }

    fn file_create(&self, uri: &uri::URI) -> Result<()> {
        fs::File::create(uri.path())?;
        Ok(())
    }

    fn file_read(
        &self,
        uri: &uri::URI,
        nbytes: u64,
        offset: u64,
        buffer: &mut [u8],
    ) -> Result<()> {
        if buffer.len() < nbytes as usize {
            let context = format!("While reading from {}", uri.to_string());
            return Err(anyhow!(
                "Unable to read {} bytes into buffer with length {}",
                nbytes,
                buffer.len()
            )
            .context(context));
        }

        self.open_file(uri)?.read_at(offset, buffer)?;

        Ok(())
    }

    fn file_read_vec(
        &self,
        uri: &uri::URI,
        nbytes: u64,
        offset: u64,
    ) -> Result<Vec<u8>> {
        let to_read = if nbytes == u64::MAX {
            self.file_size(uri)?
        } else {
            nbytes
        };

        let mut ret = vec![0; to_read as usize];
        self.file_read(uri, to_read, offset, &mut ret)?;
        Ok(ret)
    }

    fn file_write(
        &self,
        uri: &uri::URI,
        offset: u64,
        buffer: &[u8],
    ) -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(uri.path())
            .map_err(|err| {
                let context = format!("{:?}", err);
                anyhow!("Error opening file for writing: {}", uri.to_string())
            })?;
        file.write_all_at(offset, buffer).map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error writing data to {}", uri.to_string())
        })?;
        Ok(())
    }

    fn file_move(&self, src_uri: &uri::URI, dst_uri: &uri::URI) -> Result<()> {
        Ok(fs::rename(src_uri.path(), dst_uri.path())?)
    }

    fn file_copy(
        &self,
        _src_uri: &uri::URI,
        _dst_uri: &uri::URI,
    ) -> Result<()> {
        unimplemented!("Not implemented.")
    }

    fn file_sync(&self, uri: &uri::URI) -> Result<()> {
        Ok(self.open_file(uri)?.sync_data()?)
    }

    fn file_remove(&self, uri: &uri::URI) -> Result<()> {
        Ok(fs::remove_file(uri.path())?)
    }

    fn ls(&self, uri: &uri::URI) -> Result<Vec<FSEntry>> {
        let mut ret = Vec::new();
        let reader = fs::read_dir(uri.path()).map_err(|err| {
            let context = format!("{:}?", err);
            anyhow!("Error reading directory: {}", uri.to_string())
                .context(context)
        })?;
        for entry in reader {
            if entry.is_err() {
                let context = format!("{:?}", entry);
                return Err(anyhow!(
                    "Error reading directory {}",
                    uri.to_string()
                ));
            }
            let entry = entry.unwrap();

            let os_path = entry.file_name().to_string_lossy().to_string();
            let uri = uri::URI::from_string(&os_path).map_err(|err| {
                let context = format!("{:?}", err);
                anyhow!(
                    "Error converting file name to URI: {:?}",
                    entry.file_name()
                )
                .context(context)
            })?;

            let md = entry.metadata().map_err(|err| {
                let context = format!("{:?}", err);
                anyhow!("Error reading directory {}", uri.to_string())
                    .context(context)
            })?;

            let entry_type = if md.is_dir() {
                FSEntryType::Dir
            } else if md.is_file() {
                FSEntryType::File
            } else {
                FSEntryType::Unknown
            };

            let fsentry = FSEntry::new(uri, entry_type, md.len());
            ret.push(fsentry);
        }

        Ok(ret)
    }

    fn walk_files<F>(&self, _uri: &uri::URI, _f: F) -> Result<()>
    where
        F: FnMut(&FSEntry) -> Result<bool>,
    {
        unimplemented!("Not implemented.")
    }
}
