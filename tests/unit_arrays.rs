use anyhow::Result;
use tdbtk::io::service::{VFSService, WalkOptions};
use tdbtk::io::{uri, FSEntryType, PosixVFSService};
use tdbtk::storage::schema;

const UNIT_ARRAY_DIR: &str = "/Users/davisp/github/tiledb/unit-test-arrays";

#[test]
fn parse_all_schema() -> Result<()> {
    let uri_str = "file://".to_string() + UNIT_ARRAY_DIR;
    let uri = uri::URI::from_string(&uri_str)?;
    let vfs = PosixVFSService::default();

    let opts = WalkOptions::default()
        .set_min_depth(2)
        .set_max_depth(2)
        .set_sort_filenames(true);

    vfs.walk_with_options(&uri, &opts, &mut |entry| -> Result<bool> {
        let path = entry.uri().path().clone();
        if path.contains("encryption") {
            return Ok(true);
        }

        if !matches!(entry.entry_type(), FSEntryType::Dir) {
            return Ok(true);
        }

        let mut schemas = Vec::new();
        let old_uri = entry.uri().join("__array_schema.tdb");
        let dir_uri = entry.uri().join("__schema");
        if vfs.file_exists(&old_uri)? {
            schemas.push(old_uri);
        }

        vfs.walk(&dir_uri, &mut |entry| -> Result<bool> {
            if matches!(entry.entry_type(), FSEntryType::File) {
                schemas.push(entry.uri());
            }
            Ok(true)
        })?;

        for uri in schemas {
            println!("URI: {}", uri);
            let _ = schema::read_schema(&uri);
            //println!("Schema: {:?}", s);
        }

        Ok(true)
    })?;

    Ok(())
}
