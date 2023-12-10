// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::env;
use std::fmt;
use std::path;

use anyhow::{anyhow, Result};
use iref::uri::{Authority, Path, SchemeBuf};
use iref::{Uri, UriBuf};

pub enum URIType {
    Azure,
    File,
    Gcs,
    Hdfs,
    Mem,
    TileDB,
    Unknown,
}

#[derive(Clone)]
pub struct URI {
    scheme: String,
    authority: String,
    path: String,
}

impl URI {
    pub fn new() -> URI {
        URI {
            scheme: "file".to_string(),
            authority: "".to_string(),
            path: "/".to_string(),
        }
    }

    pub fn from_string(uri: &str) -> Result<URI> {
        let uri = if !uri.contains("://") {
            let p = path::Path::new(&URI::cwd()).join(uri);
            "file://".to_string() + &p.to_string_lossy()
        } else {
            uri.to_string()
        };

        let parsed = Uri::new(&uri).map_err(|err| {
            let context = format!("{:?}", err);
            anyhow!("Error parsing URI").context(context)
        })?;

        if parsed.query().is_some() {
            let context = format!("Invalid URI: {}", uri);
            return Err(
                anyhow!("Invalid URI contains a query").context(context)
            );
        }

        if parsed.fragment().is_some() {
            let context = format!("Invalid URI: {}", uri);
            return Err(
                anyhow!("Invalid URI contains a fragment").context(context)
            );
        }

        Ok(URI {
            scheme: parsed.scheme().as_str().to_string(),
            authority: parsed
                .authority()
                .map_or("".to_string(), |a| a.as_str().to_string()),
            path: parsed.path().as_str().to_string(),
        })
    }

    pub fn scheme(&self) -> String {
        self.scheme.clone()
    }

    pub fn authority(&self) -> String {
        self.authority.clone()
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn path_ref(&self) -> &String {
        &self.path
    }

    pub fn last_path_part(&self) -> String {
        if let Some(last_slash) = self.path.rfind('/') {
            self.path[last_slash + 1..].to_string()
        } else {
            self.path.clone()
        }
    }

    pub fn remove_trailing_slash(&self) -> URI {
        let mut new_path = self.path.clone();

        // PJD: I bet this is super bad creating and destroying strings
        // repeatedly. Plus CPU slow because UTF-8. I bet something like
        // create an iterator, reverse, it drop slashes, reverse again
        // and join would likely be better. Eventually I'm guessing that'll
        // be a lot easier so whatevs for now.
        while new_path.ends_with('/') {
            new_path = new_path[..new_path.len() - 1].to_string();
        }

        URI {
            scheme: self.scheme.clone(),
            authority: self.authority.clone(),
            path: new_path,
        }
    }

    pub fn join(&self, path: &str) -> URI {
        let new_path = if self.path.ends_with('/') {
            self.path.clone() + path
        } else {
            self.path.clone() + "/" + path
        };

        URI {
            scheme: self.scheme.clone(),
            authority: self.authority.clone(),
            path: new_path,
        }
    }

    pub fn uri_type(&self) -> URIType {
        match self.scheme.as_str() {
            "azure" => URIType::Azure,
            "file" => URIType::File,
            "gcs" => URIType::Gcs,
            "gs" => URIType::Gcs,
            "hdfs" => URIType::Hdfs,
            "mem" => URIType::Mem,
            "tiledb" => URIType::TileDB,
            _ => URIType::Unknown,
        }
    }

    fn cwd() -> String {
        match env::current_dir() {
            Ok(path_buf) => path_buf.as_path().to_string_lossy().to_string(),
            Err(_) => "/".to_string(),
        }
    }
}

impl Default for URI {
    fn default() -> URI {
        URI {
            scheme: "file".to_string(),
            authority: "".to_string(),
            path: Self::cwd(),
        }
    }
}

impl fmt::Display for URI {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let scheme = SchemeBuf::new(self.scheme.as_bytes().to_vec());
        if scheme.is_err() {
            write!(f, "<INVALID_URI_SCHEME:{}>", self.scheme)?;
            return Ok(());
        }
        let scheme = scheme.unwrap();

        let authority = if self.authority.is_empty() {
            None
        } else {
            match Authority::new(&self.authority) {
                Ok(auth) => Some(auth),
                Err(_) => {
                    write!(f, "<INVALID_URI_AUTHORITY:{}>", self.authority)?;
                    return Ok(());
                }
            }
        };

        let path = Path::new(&self.path);
        if path.is_err() {
            write!(f, "<INVALID_URI_PATH:{}>", self.path)?;
            return Ok(());
        }
        let path = path.unwrap();

        let mut buf = UriBuf::from_scheme(scheme);
        buf.set_authority(authority);
        buf.set_path(path);

        write!(f, "{}", buf.into_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_tests() -> Result<()> {
        let tests = vec![
            (
                "azure://bucket/path/to/array",
                "azure",
                "bucket",
                "/path/to/array",
            ),
            ("file:///path/to/array", "file", "", "/path/to/array"),
            ("file:///C:/path/to/array", "file", "", "/C:/path/to/array"),
            (
                "gs://bucket/path/to/array",
                "gs",
                "bucket",
                "/path/to/array",
            ),
            (
                "gcs://bucket/path/to/array",
                "gcs",
                "bucket",
                "/path/to/array",
            ),
            ("hdfs:///path/to/array", "hdfs", "", "/path/to/array"),
            ("mem:///path/to/array", "mem", "", "/path/to/array"),
            (
                "s3://bucket/path/to/array",
                "s3",
                "bucket",
                "/path/to/array",
            ),
            (
                "tiledb://namespace/array_name",
                "tiledb",
                "namespace",
                "/array_name",
            ),
        ];

        for test in tests {
            let uri = URI::from_string(test.0)?;
            assert_eq!(uri.scheme(), test.1);
            assert_eq!(uri.authority(), test.2);
            assert_eq!(uri.path(), test.3);
        }

        Ok(())
    }
}
