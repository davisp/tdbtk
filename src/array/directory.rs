// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use crate::io::service::WalkOptions;
use crate::io::{service, uri, FSEntry};
use crate::Result;

const SCHEMA_DIR: &str = "__schema";
const COMMITS_DIR: &str = "__commits";
const FRAGMENTS_DIR: &str = "__fragments";
//const DIMENSION_LABELS_DIR: &str = "__dimension_labels";
const ENUMERATIONS_DIR: &str = "__enumerations";

//const FILE_SUFFIX: &str = ".tdb";
const OK_FILE_SUFFIX: &str = ".ok";
const WRITE_FILE_SUFFIX: &str = ".wrt";

const OLD_SCHEMA_NAME: &str = "__array_schema.tdb";

pub struct Directory {
    array_uri: uri::URI,
    commit_entries: Vec<FSEntry>,
    root_entries: Vec<FSEntry>,
    schema_entries: Vec<FSEntry>,
}

impl Directory {
    pub fn new(array_uri: &uri::URI) -> Self {
        Directory {
            array_uri: array_uri.clone(),
            commit_entries: Vec::new(),
            root_entries: Vec::new(),
            schema_entries: Vec::new(),
        }
    }

    pub fn load_all(&mut self, vfs: &dyn service::VFSService) -> Result<()> {
        let wopts = WalkOptions::default().set_min_depth(1).set_max_depth(1);

        vfs.walk_with_options(&self.array_uri, &wopts, &mut |entry| {
            self.root_entries.push(entry.clone());
            Ok(true)
        })?;

        let commits_uri = self.array_uri.join(COMMITS_DIR);
        vfs.walk_with_options(&commits_uri, &wopts, &mut |entry| {
            self.commit_entries.push(entry.clone());
            Ok(true)
        })?;

        let schema_uri = self.array_uri.join(SCHEMA_DIR);
        vfs.walk_with_options(&schema_uri, &wopts, &mut |entry| {
            self.schema_entries.push(entry.clone());
            Ok(true)
        })?;

        Ok(())
    }

    pub fn schema_uris(&self) -> Vec<uri::URI> {
        let mut ret: Vec<uri::URI> = Vec::new();

        for entry in self.root_entries.iter() {
            if entry.uri().last_path_part() == OLD_SCHEMA_NAME {
                ret.push(entry.uri());
                break;
            }
        }

        for entry in self.schema_entries.iter() {
            if entry.uri().last_path_part() != ENUMERATIONS_DIR {
                ret.push(entry.uri());
            }
        }

        ret
    }

    // PJD: This one feels like it could be greatly simplified by using the
    // more functional approach with filters and maps and chained iterator
    // collection or w/e. However, I failed my first attempt so I'll come back
    // to it later.
    pub fn fragment_uris(&self) -> Vec<uri::URI> {
        let mut fragments: Vec<uri::URI> = Vec::new();

        for entry in self.root_entries.iter() {
            if !entry.uri().path().ends_with(OK_FILE_SUFFIX) {
                continue;
            }

            let path = entry.uri().path();
            path.strip_suffix(OK_FILE_SUFFIX)
                .expect("Error removing suffix from string with suffix");
            let mut new_uri = entry.uri().clone();
            new_uri.set_path(&path);
            fragments.push(new_uri);
        }

        for entry in self.commit_entries.iter() {
            if !entry.uri().path().ends_with(WRITE_FILE_SUFFIX) {
                continue;
            }

            let path = entry.uri().path();
            path.strip_suffix(WRITE_FILE_SUFFIX)
                .expect("Error removing suffix from string with suffix");

            let new_uri = self.array_uri.join(FRAGMENTS_DIR).join(&path);
            fragments.push(new_uri);
        }

        fragments
    }
}
