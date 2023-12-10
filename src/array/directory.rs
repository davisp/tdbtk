// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use anyhow::anyhow;

use crate::io::service::WalkOptions;
use crate::io::{service, uri, FSEntry, FSEntryType};
use crate::Result;

const SCHEMA_DIR: &str = "__schema";
const COMMITS_DIR: &str = "__commits";
const METADATA_DIR: &str = "__meta";
const FRAGMENTS_DIR: &str = "__fragments";
const FRAGMENT_METADATA_DIR: &str = "__fragment_meta";
const DIMENSION_LABELS_DIR: &str = "__dimension_labels";
const ENUMERATIONS_DIR: &str = "__enumerations";

const FILE_SUFFIX: &str = ".tdb";
const VACUUM_FILE_SUFFIX: &str = ".vac";
const OK_FILE_SUFFIX: &str = ".ok";
const WRITE_FILE_SUFFIX: &str = ".wrt";
const DELETE_FILE_SUFFIX: &str = ".del";
const UPDATE_FILE_SUFFIX: &str = ".upd";
const METADATA_FILE_SUFFIX: &str = ".meta";
const CONSOLIDATED_COMMITS_FILE_SUFFIX: &str = ".con";
const IGNORE_FILE_SUFFIX: &str = ".ign";

pub struct Directory {
    array_uri: uri::URI,
    commit_entries: Vec<FSEntry>,
    fragment_entries: Vec<FSEntry>,
    fragment_metadata_entries: Vec<FSEntry>,
    metadata_entries: Vec<FSEntry>,
    root_entries: Vec<FSEntry>,
    schema_entries: Vec<FSEntry>,
}

impl Directory {
    pub fn new(array_uri: &uri::URI) -> Result<Self> {
        Ok(Directory {
            array_uri: array_uri.clone(),
            commit_entries: Vec::new(),
            fragment_entries: Vec::new(),
            fragment_metadata_entries: Vec::new(),
            metadata_entries: Vec::new(),
            root_entries: Vec::new(),
            schema_entries: Vec::new(),
        })
    }

    pub fn load_all(
        &mut self,
        vfs: Box<dyn service::VFSService>,
    ) -> Result<()> {
        let wopts = WalkOptions::default().set_min_depth(1).set_max_depth(1);

        let commits_uri = self.array_uri.join(COMMITS_DIR);
        vfs.walk_with_options(&commits_uri, &wopts, &mut |entry| {
            self.commit_entries.push(entry.clone());
            Ok(true)
        })?;

        let fragments_uri = self.array_uri.join(FRAGMENTS_DIR);
        vfs.walk_with_options(&fragments_uri, &wopts, &mut |entry| {
            self.fragment_entries.push(entry.clone());
            Ok(true)
        })?;

        let fragment_meta_uri = self.array_uri.join(FRAGMENT_METADATA_DIR);
        vfs.walk_with_options(&fragment_meta_uri, &wopts, &mut |entry| {
            self.fragment_metadata_entries.push(entry.clone());
            Ok(true)
        })?;

        let metadata_uri = self.array_uri.join(METADATA_DIR);
        vfs.walk_with_options(&metadata_uri, &wopts, &mut |entry| {
            self.metadata_entries.push(entry.clone());
            Ok(true)
        })?;

        vfs.walk_with_options(&self.array_uri, &wopts, &mut |entry| {
            self.root_entries.push(entry.clone());
            Ok(true)
        })?;

        let schema_uri = self.array_uri.join(SCHEMA_DIR);
        vfs.walk_with_options(&schema_uri, &wopts, &mut |entry| {
            self.schema_entries.push(entry.clone());
            Ok(true)
        })?;

        let ignored = self.load_ignore_files(vfs, &self.commit_entries);

        Ok(())
    }

    fn load_ignore_files(
        &self,
        vfs: Box<dyn service::VFSService>,
        commit_entries: &[FSEntry],
    ) -> Result<Vec<uri::URI>> {
        let mut ignore_uris = Vec::new();
        for entry in commit_entries.iter() {
            if !matches!(entry.entry_type(), FSEntryType::File) {
                continue;
            }

            if !entry.uri().path_ref().ends_with(IGNORE_FILE_SUFFIX) {
                continue;
            }

            // Load the ignore file and parse the contained URIs.
            let contents = vfs
                .file_read_vec(&entry.uri(), entry.size(), 0)
                .map_err(|err| {
                    let context = format!("{:?}", err);
                    anyhow!("Error loading ignore file URI: {}", entry.uri())
                        .context(context)
                })?;

            let contents = String::from_utf8(contents).map_err(|err| {
                let context = format!("{:?}", err);
                anyhow!("Contents of {} are not valid UTF-8", entry.uri())
                    .context(context)
            })?;

            let lines = contents.lines().collect::<Vec<_>>();
            for line in lines {
                let uri = uri::URI::from_string(line).map_err(|err| {
                    let context = format!("{:?}", err);
                    anyhow!(
                        "Error parsing URI {} in ignore file {}",
                        line,
                        entry.uri()
                    )
                    .context(context)
                })?;

                ignore_uris.push(uri);
            }
        }

        Ok(ignore_uris)
    }

    fn load_consolidated_commits_uris(
        &self,
        vfs: Box<dyn service::VFSService>,
        commit_entries: &[FSEntry],
    ) -> Result<Vec<uri::URI>> {
        let mut cons_comm_uris = Vec::new();

        for entry in commit_entries.iter() {
            if !matches!(entry.entry_type(), FSEntryType::File) {
                continue;
            }

            if !entry
                .uri()
                .path_ref()
                .ends_with(CONSOLIDATED_COMMITS_FILE_SUFFIX)
            {
                continue;
            }
        }

        Ok(cons_comm_uris)
    }
}
