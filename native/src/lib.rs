//! Beacon plugin for the ODV (Ocean Data View) spreadsheet file format.

mod error;
mod format;
mod reader;
mod writer;

pub type OdvResult<T> = std::result::Result<T, error::OdvError>;

beacon_plugin_sdk::export_plugin!(formats = format::formats);
