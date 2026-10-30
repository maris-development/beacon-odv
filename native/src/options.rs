//! The write options of the `odv` format, as flat table options.
//!
//! Global options have a bare key, for example `key_column`. Column options have the key
//! `<option>::<column>`, as the column options of the DataFusion Parquet writer.

use std::collections::HashMap;

use beacon_plugin_sdk::datafusion::error::{DataFusionError, Result};

use crate::writer::{ArchivingMethod, ColumnInfo, CompressionType, OdvOptions};

fn config_error(message: String) -> DataFusionError {
    DataFusionError::Configuration(message)
}

fn column(name: &str) -> ColumnInfo {
    ColumnInfo {
        column_name: name.trim().to_string(),
        comment: None,
        significant_digits: None,
        qf_column: None,
        units: None,
    }
}

fn columns(list: &str) -> Vec<ColumnInfo> {
    list.split(',')
        .filter(|name| !name.trim().is_empty())
        .map(column)
        .collect()
}

/// Makes the write options from the table options, or `None` if there are no options.
///
/// Beacon gives the keys in lower case. A left-out option keeps its default.
pub fn write_options(options: &HashMap<String, String>) -> Result<Option<OdvOptions>> {
    if options.is_empty() {
        return Ok(None);
    }
    let mut odv = OdvOptions::default();
    let mut column_options = Vec::new();
    for (key, value) in options {
        let key = key.to_ascii_lowercase();
        if let Some((option, name)) = key.split_once("::") {
            if !["units", "comment", "significant_digits", "qf_column"].contains(&option) {
                return Err(config_error(format!("unknown ODV option `{option}`")));
            }
            column_options.push((option.to_string(), name.trim().to_string(), value));
            continue;
        }
        match key.as_str() {
            "key_column" => odv.key_column = value.trim().to_string(),
            "longitude_column" => odv.longitude_column = column(value),
            "latitude_column" => odv.latitude_column = column(value),
            "depth_column" => odv.depth_column = column(value),
            "time_column" => odv.time_column = column(value),
            "data_columns" => odv.data_columns = columns(value),
            "meta_columns" => odv.meta_columns = columns(value),
            "feature_type_column" => odv.feature_type_column = Some(value.trim().to_string()),
            "qf_schema" => odv.qf_schema = value.trim().to_string(),
            "compression" => {
                odv.compression = match value.trim().to_ascii_lowercase().as_str() {
                    "stored" => CompressionType::Stored,
                    "deflate" => CompressionType::Deflate,
                    "zstd" => CompressionType::Zstd,
                    _ => {
                        return Err(config_error(format!(
                            "invalid compression `{value}`: use stored, deflate or zstd"
                        )));
                    }
                }
            }
            "archiving" => {
                odv.archiving = match value.trim().to_ascii_lowercase().as_str() {
                    "zip_std" => ArchivingMethod::ZipStd,
                    "zip_deflate" => ArchivingMethod::ZipDeflate,
                    _ => {
                        return Err(config_error(format!(
                            "invalid archiving `{value}`: use zip_std or zip_deflate"
                        )));
                    }
                }
            }
            _ => return Err(config_error(format!("unknown ODV option `{key}`"))),
        }
    }
    // Apply the column options after the roles, because a hash map has no order.
    for (option, name, value) in column_options {
        set_column_option(&mut odv, &option, &name, value)?;
    }
    Ok(Some(odv))
}

/// Sets `option` on each role column whose name is `name`, without regard to case.
fn set_column_option(odv: &mut OdvOptions, option: &str, name: &str, value: &str) -> Result<()> {
    let mut targets: Vec<&mut ColumnInfo> = [
        &mut odv.longitude_column,
        &mut odv.latitude_column,
        &mut odv.depth_column,
        &mut odv.time_column,
    ]
    .into_iter()
    .chain(odv.data_columns.iter_mut())
    .chain(odv.meta_columns.iter_mut())
    .filter(|info| info.column_name.to_lowercase() == name)
    .collect();
    let Some(first) = targets.first() else {
        return Err(config_error(format!(
            "`{option}::{name}`: the column `{name}` has no role; name it in a *_column or *_columns option"
        )));
    };
    let exact = first.column_name.clone();
    if let Some(other) = targets.iter().find(|info| info.column_name != exact) {
        return Err(config_error(format!(
            "`{option}::{name}` matches the columns `{exact}` and `{}`, which differ only in case",
            other.column_name
        )));
    }
    let value = value.trim().to_string();
    let digits = if option == "significant_digits" {
        Some(value.parse::<u32>().map_err(|_| {
            config_error(format!(
                "invalid significant_digits::{name} `{value}`: use a whole number"
            ))
        })?)
    } else {
        None
    };
    for info in targets.iter_mut() {
        match option {
            "units" => info.units = Some(value.clone()),
            "comment" => info.comment = Some(value.clone()),
            "qf_column" => info.qf_column = Some(value.clone()),
            _ => info.significant_digits = digits,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::write_options;
    use crate::writer::{ArchivingMethod, CompressionType, OdvOptions};

    /// The options as Beacon gives them: keys in lower case.
    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
            .collect()
    }

    fn parsed(pairs: &[(&str, &str)]) -> OdvOptions {
        write_options(&options(pairs)).unwrap().expect("options")
    }

    fn error(pairs: &[(&str, &str)]) -> String {
        write_options(&options(pairs))
            .expect_err("an error")
            .to_string()
    }

    /// The full example of the README.
    const EXAMPLE: &[(&str, &str)] = &[
        ("KEY_COLUMN", "Cruise"),
        ("LONGITUDE_COLUMN", "Longitude"),
        ("LATITUDE_COLUMN", "Latitude"),
        ("DEPTH_COLUMN", "Depth"),
        ("TIME_COLUMN", "yyyy-mm-ddThh:mm:ss.sss"),
        ("DATA_COLUMNS", "Temperature, Salinity"),
        ("META_COLUMNS", "Station, Type, EDMO_code"),
        ("FEATURE_TYPE_COLUMN", "featureType"),
        ("QF_SCHEMA", "SEADATANET"),
        ("COMPRESSION", "deflate"),
        ("ARCHIVING", "zip_std"),
        ("UNITS::Depth", "m"),
        ("UNITS::Temperature", "degrees C"),
        ("COMMENT::Temperature", "ITS-90 water temperature"),
        ("SIGNIFICANT_DIGITS::Temperature", "3"),
        ("QF_COLUMN::Temperature", "Temperature_qc"),
        ("UNITS::Salinity", "PSU"),
        ("SIGNIFICANT_DIGITS::Salinity", "4"),
        ("QF_COLUMN::Salinity", "Salinity_qc"),
    ];

    #[test]
    fn no_options_give_none() {
        assert!(write_options(&HashMap::new()).unwrap().is_none());
    }

    #[test]
    fn the_readme_example_sets_every_field() {
        let o = parsed(EXAMPLE);
        assert_eq!(o.key_column, "Cruise");
        assert_eq!(o.longitude_column.column_name, "Longitude");
        assert_eq!(o.latitude_column.column_name, "Latitude");
        assert_eq!(o.time_column.column_name, "yyyy-mm-ddThh:mm:ss.sss");
        assert_eq!(o.depth_column.column_name, "Depth");
        assert_eq!(o.depth_column.units.as_deref(), Some("m"));
        assert_eq!(o.feature_type_column.as_deref(), Some("featureType"));
        assert_eq!(o.qf_schema, "SEADATANET");
        assert!(matches!(o.compression, CompressionType::Deflate));
        assert!(matches!(o.archiving, ArchivingMethod::ZipStd));

        let names: Vec<&str> = o
            .meta_columns
            .iter()
            .map(|c| c.column_name.as_str())
            .collect();
        assert_eq!(names, ["Station", "Type", "EDMO_code"]);

        let temperature = &o.data_columns[0];
        assert_eq!(temperature.column_name, "Temperature");
        assert_eq!(temperature.units.as_deref(), Some("degrees C"));
        assert_eq!(
            temperature.comment.as_deref(),
            Some("ITS-90 water temperature")
        );
        assert_eq!(temperature.significant_digits, Some(3));
        assert_eq!(temperature.qf_column.as_deref(), Some("Temperature_qc"));
        let salinity = &o.data_columns[1];
        assert_eq!(salinity.column_name, "Salinity");
        assert_eq!(salinity.units.as_deref(), Some("PSU"));
        assert_eq!(salinity.significant_digits, Some(4));
        assert_eq!(o.data_columns.len(), 2);
    }

    #[test]
    fn left_out_options_keep_the_defaults() {
        let o = parsed(&[("KEY_COLUMN", "Station")]);
        let default = OdvOptions::default();
        assert_eq!(o.key_column, "Station");
        assert_eq!(o.time_column.column_name, default.time_column.column_name);
        assert_eq!(o.depth_column.column_name, default.depth_column.column_name);
        assert_eq!(o.qf_schema, default.qf_schema);
        assert!(o.data_columns.is_empty());
    }

    #[test]
    fn upper_case_keys_also_work() {
        let pairs: HashMap<String, String> = [("KEY_COLUMN", "Station"), ("UNITS::DEPTH", "m")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut pairs = pairs;
        pairs.insert("DEPTH_COLUMN".to_string(), "Depth".to_string());
        let o = write_options(&pairs).unwrap().unwrap();
        assert_eq!(o.key_column, "Station");
        assert_eq!(o.depth_column.units.as_deref(), Some("m"));
    }

    #[test]
    fn column_options_match_without_case() {
        let o = parsed(&[("DATA_COLUMNS", "Temperature"), ("units::TEMPERATURE", "K")]);
        assert_eq!(o.data_columns[0].units.as_deref(), Some("K"));
    }

    #[test]
    fn empty_list_items_are_skipped() {
        let o = parsed(&[("DATA_COLUMNS", " Temperature , ,Salinity, ")]);
        let names: Vec<&str> = o
            .data_columns
            .iter()
            .map(|c| c.column_name.as_str())
            .collect();
        assert_eq!(names, ["Temperature", "Salinity"]);
    }

    #[test]
    fn enum_values_are_not_case_sensitive() {
        let o = parsed(&[("COMPRESSION", "ZSTD"), ("ARCHIVING", "Zip_Deflate")]);
        assert!(matches!(o.compression, CompressionType::Zstd));
        assert!(matches!(o.archiving, ArchivingMethod::ZipDeflate));
        assert!(matches!(
            parsed(&[("COMPRESSION", "stored")]).compression,
            CompressionType::Stored
        ));
    }

    #[test]
    fn unknown_option_is_an_error() {
        assert!(error(&[("KEY_COLUMS", "Cruise")]).contains("key_colums"));
        assert!(error(&[("COLOR::Depth", "red")]).contains("color"));
    }

    #[test]
    fn column_option_without_a_role_is_an_error() {
        let message = error(&[("DATA_COLUMNS", "Temperature"), ("UNITS::Oxygen", "umol/l")]);
        assert!(message.contains("oxygen"), "{message}");
    }

    #[test]
    fn significant_digits_must_be_a_number() {
        let message = error(&[
            ("DATA_COLUMNS", "Temperature"),
            ("SIGNIFICANT_DIGITS::Temperature", "three"),
        ]);
        assert!(message.contains("significant_digits"), "{message}");
    }

    #[test]
    fn bad_enum_values_are_errors() {
        assert!(error(&[("COMPRESSION", "lz4")]).contains("compression"));
        assert!(error(&[("ARCHIVING", "tar")]).contains("archiving"));
    }

    #[test]
    fn columns_that_differ_only_in_case_are_an_error() {
        let message = error(&[("DATA_COLUMNS", "Temp, TEMP"), ("UNITS::temp", "K")]);
        assert!(message.contains("temp"), "{message}");
    }
}
