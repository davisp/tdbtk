// This file is part of tdbtk released under the MIT license.
// Copyright (c) 2023 TileDB, Inc.

use std::mem::size_of;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default)]
pub enum DataType {
    #[default]
    Int32 = 0,
    Int64 = 1,
    Float32 = 2,
    Float64 = 3,
    Char = 4,
    Int8 = 5,
    Uint8 = 6,
    Int16 = 7,
    Uint16 = 8,
    Uint32 = 9,
    Uint64 = 10,
    StringAscii = 11,
    StringUtf8 = 12,
    StringUtf16 = 13,
    StringUtf32 = 14,
    StringUcs2 = 15,
    StringUcs4 = 16,
    Any = 17,
    DatetimeYear = 18,
    DatetimeMonth = 19,
    DatetimeWeek = 20,
    DatetimeDay = 21,
    DatetimeHour = 22,
    DatetimeMin = 23,
    DatetimeSec = 24,
    DatetimeMSec = 25,
    DatetimeUSec = 26,
    DatetimeNSec = 27,
    DatetimePSec = 28,
    DatetimeFSec = 29,
    DatetimeASec = 30,
    TimeHour = 31,
    TimeMin = 32,
    TimeSec = 33,
    TimeMSec = 34,
    TimeUSec = 35,
    TimeNSec = 36,
    TimePSec = 37,
    TimeFSec = 38,
    TimeASec = 39,
    Blob = 40,
    Bool = 41,
    Invalid = 255,
}

impl DataType {
    pub fn size(&self) -> usize {
        match self {
            DataType::Int32 => size_of::<i32>(),
            DataType::Int64 => size_of::<i64>(),
            DataType::Float32 => size_of::<f32>(),
            DataType::Float64 => size_of::<f64>(),
            DataType::Char => size_of::<char>(),
            DataType::Int8 => size_of::<i8>(),
            DataType::Uint8 => size_of::<u8>(),
            DataType::Int16 => size_of::<i16>(),
            DataType::Uint16 => size_of::<u16>(),
            DataType::Uint32 => size_of::<u32>(),
            DataType::Uint64 => size_of::<u64>(),
            DataType::StringAscii => size_of::<char>(),
            DataType::StringUtf8 => size_of::<u8>(),
            DataType::StringUtf16 => size_of::<u16>(),
            DataType::StringUtf32 => size_of::<u32>(),
            DataType::StringUcs2 => size_of::<u16>(),
            DataType::StringUcs4 => size_of::<u32>(),
            DataType::Any => size_of::<u8>(),
            DataType::DatetimeYear => size_of::<u64>(),
            DataType::DatetimeMonth => size_of::<u64>(),
            DataType::DatetimeWeek => size_of::<u64>(),
            DataType::DatetimeDay => size_of::<u64>(),
            DataType::DatetimeHour => size_of::<u64>(),
            DataType::DatetimeMin => size_of::<u64>(),
            DataType::DatetimeSec => size_of::<u64>(),
            DataType::DatetimeMSec => size_of::<u64>(),
            DataType::DatetimeUSec => size_of::<u64>(),
            DataType::DatetimeNSec => size_of::<u64>(),
            DataType::DatetimePSec => size_of::<u64>(),
            DataType::DatetimeFSec => size_of::<u64>(),
            DataType::DatetimeASec => size_of::<u64>(),
            DataType::TimeHour => size_of::<u64>(),
            DataType::TimeMin => size_of::<u64>(),
            DataType::TimeSec => size_of::<u64>(),
            DataType::TimeMSec => size_of::<u64>(),
            DataType::TimeUSec => size_of::<u64>(),
            DataType::TimeNSec => size_of::<u64>(),
            DataType::TimePSec => size_of::<u64>(),
            DataType::TimeFSec => size_of::<u64>(),
            DataType::TimeASec => size_of::<u64>(),
            DataType::Blob => size_of::<u8>(),
            DataType::Bool => size_of::<u8>(),
            DataType::Invalid => 0,
        }
    }

    pub fn is_string_type(&self) -> bool {
        matches!(
            self,
            DataType::StringAscii
                | DataType::StringUtf8
                | DataType::StringUtf16
                | DataType::StringUtf32
                | DataType::StringUcs2
                | DataType::StringUcs4
        )
    }
}

impl From<u8> for DataType {
    fn from(orig: u8) -> Self {
        match orig {
            0 => DataType::Int32,
            1 => DataType::Int64,
            2 => DataType::Float32,
            3 => DataType::Float64,
            4 => DataType::Char,
            5 => DataType::Int8,
            6 => DataType::Uint8,
            7 => DataType::Int16,
            8 => DataType::Uint16,
            9 => DataType::Uint32,
            10 => DataType::Uint64,
            11 => DataType::StringAscii,
            12 => DataType::StringUtf8,
            13 => DataType::StringUtf16,
            14 => DataType::StringUtf32,
            15 => DataType::StringUcs2,
            16 => DataType::StringUcs4,
            17 => DataType::Any,
            18 => DataType::DatetimeYear,
            19 => DataType::DatetimeMonth,
            20 => DataType::DatetimeWeek,
            21 => DataType::DatetimeDay,
            22 => DataType::DatetimeHour,
            23 => DataType::DatetimeMin,
            24 => DataType::DatetimeSec,
            25 => DataType::DatetimeMSec,
            26 => DataType::DatetimeUSec,
            27 => DataType::DatetimeNSec,
            28 => DataType::DatetimePSec,
            29 => DataType::DatetimeFSec,
            30 => DataType::DatetimeASec,
            31 => DataType::TimeHour,
            32 => DataType::TimeMin,
            33 => DataType::TimeSec,
            34 => DataType::TimeMSec,
            35 => DataType::TimeUSec,
            36 => DataType::TimeNSec,
            37 => DataType::TimePSec,
            38 => DataType::TimeFSec,
            39 => DataType::TimeASec,
            40 => DataType::Blob,
            41 => DataType::Bool,
            _ => DataType::Invalid,
        }
    }
}
