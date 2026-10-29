//! Beacon plugin for the ODV (Ocean Data View) spreadsheet file format.

mod error;
mod format;
// The ODV code comes from Beacon. Keep its unused API to ease a later comparison.
#[allow(dead_code)]
mod reader;
#[allow(dead_code)]
mod writer;

pub type OdvResult<T> = std::result::Result<T, error::OdvError>;

beacon_plugin_sdk::export_plugin!(formats = format::formats);
