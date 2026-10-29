//! The `odv` file format of the plugin SDK.

use std::sync::Arc;

use beacon_plugin_sdk::file_format::FormatFactory;

/// The file formats of this plugin.
pub fn formats() -> Vec<Arc<dyn FormatFactory>> {
    Vec::new()
}
