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

The table option `odv_options` sets the write options as JSON:

```sql
CREATE EXTERNAL TABLE out STORED AS ODV LOCATION 'out/'
OPTIONS ('odv_options' '{"key_column": "Cruise", "qf_schema": "SEADATANET", ...}');
```

The JSON must have all the required fields:

| Field | Required | Contents |
| --- | --- | --- |
| `longitude_column` | yes | The longitude column |
| `latitude_column` | yes | The latitude column |
| `depth_column` | yes | The depth or pressure column |
| `time_column` | yes | The time column |
| `key_column` | yes | The column that identifies a cruise or deployment |
| `qf_schema` | yes | The quality flag schema, for example `SEADATANET` |
| `data_columns` | yes | The data columns. Can be empty. |
| `meta_columns` | yes | The metadata columns. Can be empty. The alias is `metadata_columns`. |
| `feature_type_column` | no | A column that sets the ODV type of each row |
| `archiving` | no | `ZipStd` (default) or `zip_deflate` |
| `compression` | no | `Stored`, `Deflate` (default) or `Zstd` |

Each column field is an object:

| Field | Contents |
| --- | --- |
| `column_name` | The column name. Required. |
| `comment` | A comment for the ODV header |
| `significant_digits` | The number of significant digits for numbers |
| `qf_column` | The quality flag column of this column |
| `units` | The units. The alias is `unit`. |

Without `odv_options`, the writer finds the options from the column names.
It uses these default names: `Longitude [degrees east]`, `Latitude [degrees north]`, `Depth [m]`, `yyyy-MM-ddTHH:mm:ss.SSS` and `Cruise`.
A table read from an ODV file has the time column `yyyy-mm-ddThh:mm:ss.sss`.
So set `odv_options` when you write data that came from ODV files.

The writer sorts the rows by the key column, the time column and the depth column.

## Limits

- Beacon does not call the writer of a plugin yet. `COPY ... TO` and the query output do not use this plugin.
  When Beacon adds this, the output file must get the extension `.zip`.
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
