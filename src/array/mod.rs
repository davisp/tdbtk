// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

pub mod schema;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum ArrayType {
    #[default]
    Dense = 0,
    Sparse = 1,
    Invalid = 255,
}

impl From<u8> for ArrayType {
    fn from(orig: u8) -> Self {
        match orig {
            0 => ArrayType::Dense,
            1 => ArrayType::Sparse,
            _ => ArrayType::Invalid,
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum Layout {
    #[default]
    RowMajor = 0,
    ColMajor = 1,
    GlobalOrder = 2,
    Unordered = 3,
    Hilbert = 4,
    Invalid = 255,
}

impl From<u8> for Layout {
    fn from(orig: u8) -> Self {
        match orig {
            0 => Layout::RowMajor,
            1 => Layout::ColMajor,
            2 => Layout::GlobalOrder,
            3 => Layout::Unordered,
            4 => Layout::Hilbert,
            _ => Layout::Invalid,
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum DataOrder {
    #[default]
    Unordered = 0,
    Increasing = 1,
    Decreasing = 2,
    Invalid = 255,
}

impl From<u8> for DataOrder {
    fn from(orig: u8) -> Self {
        match orig {
            0 => DataOrder::Unordered,
            1 => DataOrder::Increasing,
            2 => DataOrder::Decreasing,
            _ => DataOrder::Invalid,
        }
    }
}
