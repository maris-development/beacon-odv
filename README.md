# beacon-odv

A [Beacon](https://github.com/maris-development/beacon) plugin for the ODV (Ocean Data View) spreadsheet file format.
The plugin reads ODV files. It also has an ODV writer for a future Beacon version.

## Install the plugin

An admin installs the plugin from a GitHub release:

```sql
INSTALL odv FROM 'github:maris-development/beacon-odv@v0.1.0'
```

Beacon downloads the archive for its own platform, checks it against `SHA256SUMS` and loads it.
`BEACON_PLUGINS=odv=github:maris-development/beacon-odv@v0.1.0` installs the plugin at server start.
`SELECT * FROM beacon.system.plugins` shows the installed plugins. `UNINSTALL odv` removes the plugin.

The plugin needs Beacon `2.1.0-rc.1` or a later `2.x` version.

## Read ODV files

Make an external table with `STORED AS ODV`:

```sql
CREATE EXTERNAL TABLE stations STORED AS ODV LOCATION 'odv/*.txt';
SELECT * FROM stations;
```

The plugin also adds two table functions:

| Function | Result |
| --- | --- |
| `read_odv(['odv/*.txt'])` | The rows of the files |
| `read_odv_schema(['odv/*.txt'])` | The columns of the files |

The plugin reads compressed files too. It finds the compression from the file extension:

| Extension | Compression |
| --- | --- |
| `.gz` | gzip |
| `.bz2` | bzip2 |
| `.xz` | xz |
| `.zst`, `.zstd` | zstd |

### Columns

- The reader removes the units from a column name. `Longitude [degrees_east]` becomes `Longitude`.
- A quality flag column `QV:<schema>` after a column `X` becomes `X_qc`.
- The reader adds one text column `<column>.<key>` for each metadata item of a column, for example `Longitude.units`.

## Write ODV files

The plugin has an ODV writer. The writer makes one zip archive with these files:

| Archive entry | Contents |
| --- | --- |
| `profile.txt` | Profile data |
| `timeseries.txt` | Time series data |
| `trajectories.txt` | Trajectory data |
| `errors.txt` | Rows that the writer cannot classify |

Set the write options in `OPTIONS`. This example sets every option:

```sql
COPY (SELECT * FROM stations)
TO 'output/stations.zip'
STORED AS ODV
OPTIONS (
  'KEY_COLUMN'          'Cruise',
  'LONGITUDE_COLUMN'    'Longitude',
  'LATITUDE_COLUMN'     'Latitude',
  'DEPTH_COLUMN'        'Depth',
  'TIME_COLUMN'         'yyyy-mm-ddThh:mm:ss.sss',
  'DATA_COLUMNS'        'Temperature, Salinity',
  'META_COLUMNS'        'Station, Type, EDMO_code',
  'FEATURE_TYPE_COLUMN' 'featureType',
  'QF_SCHEMA'           'SEADATANET',
  'COMPRESSION'         'deflate',
  'ARCHIVING'           'zip_std',
  'UNITS::Depth'                    'm',
  'UNITS::Temperature'              'degrees C',
  'COMMENT::Temperature'            'ITS-90 water temperature',
  'SIGNIFICANT_DIGITS::Temperature' '3',
  'QF_COLUMN::Temperature'          'Temperature_qc',
  'UNITS::Salinity'                 'PSU',
  'SIGNIFICANT_DIGITS::Salinity'    '4',
  'QF_COLUMN::Salinity'             'Salinity_qc'
);
```

All options are optional. An option that you leave out gets its default.
Option keys are not case-sensitive.

| Option | Default | Value |
| --- | --- | --- |
| `KEY_COLUMN` | `Cruise` | The column that identifies a cruise or deployment |
| `LONGITUDE_COLUMN` | `Longitude [degrees east]` | The longitude column |
| `LATITUDE_COLUMN` | `Latitude [degrees north]` | The latitude column |
| `DEPTH_COLUMN` | `Depth [m]` | The depth or pressure column |
| `TIME_COLUMN` | `yyyy-MM-ddTHH:mm:ss.SSS` | The time column |
| `DATA_COLUMNS` | none | The data columns, separated by commas |
| `META_COLUMNS` | none | The metadata columns, separated by commas |
| `FEATURE_TYPE_COLUMN` | none | A column that sets the ODV type of each row |
| `QF_SCHEMA` | `SEADATANET` | The quality flag schema |
| `COMPRESSION` | `deflate` | `stored`, `deflate` or `zstd` |
| `ARCHIVING` | `zip_std` | `zip_std` or `zip_deflate` |

Column options have the key `<option>::<column>`, as the column options of the DataFusion Parquet writer.
They apply to the longitude, latitude, depth and time columns, and to the data and metadata columns:

| Option | Value |
| --- | --- |
| `UNITS::<column>` | The units of the column |
| `COMMENT::<column>` | A comment for the ODV header |
| `SIGNIFICANT_DIGITS::<column>` | The number of significant digits, a whole number |
| `QF_COLUMN::<column>` | The quality flag column of the column |

Beacon makes all option keys lower case. So the plugin finds the `<column>` of a column option without regard to case.
Beacon drops an option key that contains a dot. For a column name with a dot, write `format.` before the key, for example `'format.UNITS::Depth.raw'`.

The plugin gives an error for:

- an unknown option
- a column option for a column that is not in a column option or a column list
- a `SIGNIFICANT_DIGITS` value that is not a whole number
- a column option that matches two columns that differ only in case

Without options, the writer finds the options from the column names, with the defaults above.
A table read from an ODV file has the time column `yyyy-mm-ddThh:mm:ss.sss`.
So set `TIME_COLUMN` and the other column options when you write data that came from ODV files.

The writer sorts the rows by the key column, the time column and the depth column.

## Limits

- Beacon does not call the writer of a plugin yet. `COPY ... TO` and the query output do not use this plugin.
  When Beacon adds this, the output file must get the extension `.zip`.
  The `COPY ... TO` example above shows the planned use.
- The plugin registers the file extension `txt`. So `STORED AS TXT` also reads ODV.
- The built-in Beacon function `read_odv_ascii` has the name `read_odv` in this plugin.
- The reader does not check that a `.txt` file is ODV. A file that is not ODV can give a table with strange columns.

## Build and test

```bash
cargo test --manifest-path native/Cargo.toml
```

To release, set `plugin.version` in `beacon-plugin.toml`, commit and push a tag `v<version>`.
The release workflow builds the archives for Linux, macOS and Windows.

The ODV code comes from Beacon commit `a612fce` (`beacon-db/beacon-file-formats/beacon-arrow-odv`).

## License

Apache-2.0. See [LICENSE](LICENSE).
