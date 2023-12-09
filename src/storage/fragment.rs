// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::collections::HashMap;

use anyhow::anyhow;
use binrw::binrw;

use crate::io::uri;
use crate::storage;
use crate::Result;

#[derive(Debug, PartialEq, Eq)]
enum FragmentNameVersion {
    One,
    Two,
    Three,
}

fn get_fragment_name_version(name: &String) -> FragmentNameVersion {
    let num_underscores = name.chars().filter(|c| *c == '_').count();
    if num_underscores == 5 {
        return FragmentNameVersion::Three;
    }

    if let Some(last_underscore) = name.rfind('_') {
        if name.len() - last_underscore - 1 == 32 {
            return FragmentNameVersion::Two;
        }
    }

    FragmentNameVersion::One
}

fn get_fragment_version(name: &String) -> Result<u32> {
    let name_version = get_fragment_name_version(name);

    if name_version == FragmentNameVersion::One {
        return Ok(2);
    }

    if name_version == FragmentNameVersion::Two {
        return Ok(4);
    }

    // Else, version 3 has the version after the last underscore
    if let Some(last_underscore) = name.rfind('_') {
        name[last_underscore + 1..].parse().map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error parsing fragment version from: {}", name)
                .context(context)
        })
    } else {
        Err(anyhow!("Invalid fragment name: {}", name)
            .context("While parsing the fragment name version."))
    }
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (nfields: u32))]
struct FragmentFileOffsets {
    #[br(count(nfields))]
    fixed_sizes: Vec<u64>,

    #[br(count(nfields))]
    var_sizes: Vec<u64>,

    #[br(count(nfields))]
    validity_sizes: Vec<u64>,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (nfields: u32))]
struct FragmentTileOffsets {
    rtree: u64,

    #[br(count(nfields))]
    fixed_offsets: Vec<u64>,

    #[br(count(nfields))]
    var_offsets: Vec<u64>,

    #[br(count(nfields))]
    var_sizes: Vec<u64>,

    #[br(count(nfields))]
    validity_offsets: Vec<u64>,

    #[br(count(nfields))]
    min_offsets: Vec<u64>,

    #[br(count(nfields))]
    max_offsets: Vec<u64>,

    #[br(count(nfields))]
    sum_offsets: Vec<u64>,

    #[br(count(nfields))]
    null_count_offsets: Vec<u64>,

    frag_meta_offset: u64,

    processed_conditions_offset: u64,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (schema: storage::ArraySchema))]
struct FragmentMetadataPreFooter {
    version: u32,

    domain_size: u64,

    // Need to add a map here to decode the non-empty domain
    #[br(count(domain_size))]
    #[br(if(domain_size != 0, Vec::new()))]
    non_empty_domain: Vec<u8>,

    num_mbrs: u64,
}

#[derive(Debug)]
#[binrw]
#[brw(little)]
#[br(import (nfields: u32))]
struct FragmentFooter {
    version: u32,

    array_schema_size: u64,

    #[br(count(array_schema_size))]
    #[br(map = |v: Vec<u8>| String::from_utf8(v).unwrap())]
    #[bw(map = |n: &String| n.as_bytes().to_vec())]
    array_scheam: String,

    fragment_type: u8,

    null_non_empty_domain: u8,

    #[br(if(null_non_empty_domain == 0, Vec::new()))]
    #[br(count(4))]
    non_empty_domain: Vec<f64>,

    sparse_tile_num: u64,
    last_tile_cell_num: u64,
    has_timestamps: u8,
    has_delete_meta: u8,

    #[br(args(nfields))]
    file_offsets: FragmentFileOffsets,

    #[br(args(nfields))]
    tile_offsets: FragmentTileOffsets,
}

pub struct Fragment {
    uri: uri::URI,
    format_version: u32,
    footer: Option<FragmentFooter>,
}

impl Fragment {
    fn new(
        uri: &uri::URI,
        schemas: HashMap<String, storage::ArraySchema>,
    ) -> Result<Fragment> {
        let name = uri.remove_trailing_slash().last_path_part();
        let vsn = get_fragment_version(&name)?;

        // if vsn <= 2 {
        //     Fragment::load_v1_v2(uri, vsn, schemas)
        // } else {
        //     panic!("Still working on v1/v2 loading");
        //     //Fragment::load_v3_or_newer(uri, vsn, schemas)
        // }

        Ok(Fragment {
            uri: uri.clone(),
            format_version: vsn,
            footer: None,
        })
    }

    //     fn load_v1_v2(
    //         uri: &uri::URI,
    //         vsn: u32,
    //         schemas: HashMap<String, storage::ArraySchema>,
    //     ) -> Result<Fragment> {
    //         // Pre v10 fragments have an __array_schema.tdb as their schema.
    //         let schema = schemas.get("__array_schema.tdb");
    //         if schema.is_none() {
    //             let context =
    //                 format!("While loading fragment metadata for {}", uri);
    //             return Err(anyhow!(
    //                 "Failed finding array schema '__array_schema.tdb'"
    //             )
    //             .context(context));
    //         }
    //
    //         let schema = schema.unwrap();
    //
    //         let fmd_uri = uri.join("__fragment_metadata.tdb");
    //         let data = storage::read_generic_tile(&fmd_uri, 0);
    //
    //         Ok(Fragment {})
    //     }

    //     fn load_v3_or_newer(
    //         uri: &uri::URI,
    //         vsn: u32,
    //         schemas: HashMap<String, storage::ArraySchema>,
    //     ) -> Result<Fragment> {
    //     }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distributions::Distribution;
    use rand::{seq::SliceRandom, Rng};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct UUIDChars;

    impl Distribution<char> for UUIDChars {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> char {
            *b"0123456789abcdef".choose(rng).unwrap() as char
        }
    }

    fn generate_uuid() -> String {
        rand::thread_rng()
            .sample_iter(&UUIDChars)
            .take(32)
            .collect()
    }

    fn get_now_str() -> String {
        if let Ok(time) = SystemTime::now().duration_since(UNIX_EPOCH) {
            time.as_millis().to_string()
        } else {
            0.to_string()
        }
    }

    fn generate_v1_name() -> String {
        let ret = "__".to_string() + &generate_uuid() + "_" + &get_now_str();
        if rand::random() {
            ret + "_" + &get_now_str()
        } else {
            ret
        }
    }

    fn generate_v2_name() -> String {
        "__".to_string()
            + &get_now_str()
            + "_"
            + &get_now_str()
            + "_"
            + &generate_uuid()
    }

    fn generate_v3_name() -> (String, u32) {
        let vsn = rand::random::<u8>();
        let name = "__".to_string()
            + &get_now_str()
            + "_"
            + &get_now_str()
            + "_"
            + &generate_uuid()
            + "_"
            + &vsn.to_string();
        (name, vsn as u32)
    }

    #[test]
    fn fragment_name_version_test() {
        for _ in 1..1000 {
            let name = generate_v1_name();
            assert_eq!(
                get_fragment_name_version(&name),
                FragmentNameVersion::One
            );
            let name = generate_v2_name();
            assert_eq!(
                get_fragment_name_version(&name),
                FragmentNameVersion::Two
            );
            let name = generate_v3_name().0;
            assert_eq!(
                get_fragment_name_version(&name),
                FragmentNameVersion::Three
            );
        }
    }

    #[test]
    fn fragment_version_test() {
        for _ in 1..1000 {
            let name = generate_v1_name();
            assert_eq!(get_fragment_version(&name).unwrap(), 2);
            let name = generate_v2_name();
            assert_eq!(get_fragment_version(&name).unwrap(), 4);
            let (name, vsn) = generate_v3_name();
            assert_eq!(get_fragment_version(&name).unwrap(), vsn);
        }
    }

    #[test]
    fn fragment_version_error() {
        let (name, _) = generate_v3_name();
        let name = name + "blargh";
        assert!(get_fragment_version(&name).is_err())
    }
}
