use anyhow::Result;
use tdbtk::io::service::VFSService;
use tdbtk::io::{uri, PosixVFSService};
use tdbtk::storage::schema;

const UNIT_ARRAY_DIR: &str = "/Users/davisp/github/tiledb/unit-test-arrays";

#[test]
fn parse_all_schema() -> Result<()> {
    println!("Ohai!");
    let uri_str = "file://".to_string() + UNIT_ARRAY_DIR;
    let uri = uri::URI::from_string(&uri_str)?;
    let vfs = PosixVFSService::default();
    vfs.walk_files(&uri, &mut |entry| -> Result<bool> {
        let path = entry.uri().path().clone();
        if path.contains("encryption") {
            return Ok(true);
        }

        if path.contains("__schema") || path.contains("__array_schema.tdb") {
            println!("Reading schema: {}", entry.uri());
            let _ = schema::read(&entry.uri())?;
        }

        Ok(true)
    })?;

    Ok(())
}
