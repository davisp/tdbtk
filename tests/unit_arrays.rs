use std::collections::HashMap;

use anyhow::Result;
use tdbtk::array;
use tdbtk::io::service::{VFSService, WalkOptions};
use tdbtk::io::{uri, FSEntryType, PosixVFSService};
use tdbtk::storage;

const UNIT_ARRAY_DIR: &str = "/Users/davisp/github/tiledb/unit-test-arrays";

fn list_arrays(uri: &uri::URI, vfs: &dyn VFSService) -> Result<Vec<uri::URI>> {
    let mut ret: Vec<uri::URI> = Vec::new();

    let opts = WalkOptions::default()
        .set_min_depth(2)
        .set_max_depth(2)
        .set_sort_filenames(true);

    vfs.walk_with_options(uri, &opts, &mut |entry| -> Result<bool> {
        if !matches!(entry.entry_type(), FSEntryType::Dir) {
            return Ok(true);
        }

        if entry.uri().path().contains("encryption") {
            return Ok(true);
        }

        ret.push(entry.uri().clone());

        Ok(true)
    })?;

    Ok(ret)
}

#[test]
fn parse_all_schema() -> Result<()> {
    let uri_str = "file://".to_string() + UNIT_ARRAY_DIR;
    let uri = uri::URI::from_string(&uri_str)?;
    let vfs = PosixVFSService::default();

    let arrays = list_arrays(&uri, &vfs)?;

    for uri in arrays.iter() {
        let mut dir = array::Directory::new(uri);
        dir.load_all(&vfs)?;

        if dir.schema_uris().is_empty() {
            println!("Skipping non-array: {}", uri);
            continue;
        }

        let mut schemas: HashMap<String, array::Schema> = HashMap::new();
        for uri in dir.schema_uris() {
            let storage_schema =
                storage::ArraySchema::load(dir.schema_uris().first().unwrap())?;
            let schema = array::Schema::try_from(storage_schema)?;
            schemas.insert(uri.last_path_part(), schema);
        }

        for uri in dir.fragment_uris().iter() {
            let fmd = storage::FragmentMetadata::load(uri, &schemas);
        }
    }

    Ok(())
}
