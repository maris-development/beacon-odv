# ODV file format plugin for Beacon

## Intent

Beacon reads and writes ODV (Ocean Data View) spreadsheet files with built-in code today.
The owner wants this code as an installable Beacon plugin.
An admin installs the plugin. A user then reads ODV files through an external table:

```sql
INSTALL odv FROM 'github:<owner>/beacon-odv@v0.1.0';
CREATE EXTERNAL TABLE stations STORED AS ODV LOCATION 'data/*.txt';
SELECT * FROM stations;
```

The owner removes the built-in ODV code from the Beacon repo later, in separate work.

## Goals

1. Port the ODV reader from Beacon into this repo as a plugin file format with the name `odv`.
2. Port the ODV writer into the same plugin, on the SDK `write` method.
3. Turn this repo from the `beacon-plugin-template` example into the ODV plugin.
4. Keep the release workflow, so a `v*` tag builds the plugin for all five platforms.

## Non-goals

- No change to the Beacon repo.
- No `COPY ... TO`, query output or statistics support in the Beacon host.
- No new ODV features. The port keeps the current behavior.

## Anti-goals

- The reader must not change results. The same ODV file must give the same schema and rows as Beacon gives today.
- The writer must not change output. The same input and options must give the same files as Beacon writes today.

## Facts from recon

Source: `maris-development/beacon` at commit `a612fce` (2026-10-29), crate `beacon-db/beacon-file-formats/beacon-arrow-odv`.

| File | Lines | Content |
| --- | --- | --- |
| `src/reader.rs` | 591 | `AsyncOdvDecoder`, `OdvSchemaMapper`, `OdvObjectReader` |
| `src/writer.rs` | 2383 | `OdvOptions`, `ColumnInfo`, ODV writer, zip archive output, tests |
| `src/datafusion/mod.rs` | 285 | DataFusion `FileFormat`, compression from extension, sort before write |
| `src/datafusion/source.rs` | 239 | DataFusion `FileSource` |
| `src/datafusion/sink.rs` | 146 | DataFusion `DataSink` |
| `src/datafusion/table_function.rs` | 93 | `read_odv_ascii(glob_paths)` |
| `src/error.rs` | 29 | error types |

Test data: `test-datasets/test_file.txt` and `test-datasets/test_file.txt.zst`.

Known behavior of the current code:

- The reader removes units from column names. `Longitude [degrees_east]` becomes `Longitude`.
- The `OdvOptions` defaults expect other column names than `test_file.txt` has. `try_from_arrow_schema` compares names in lower case. The writer finds columns by exact name.
- The writer output is one zip. It holds a profile file, a time-series file, a trajectory file and an error file.
- The reader reads one plain or compressed text file. It cannot read the zip.

The crate depends on `beacon-common` and `beacon-datafusion-ext`. A plugin can depend only on `beacon-plugin-sdk` and normal crates.

The plugin SDK (`beacon-db/beacon-plugin-sdk/src/file_format`) gives:

- `FormatFactory`: `name`, `extensions`, `output_kind`, `table_function_args`, `can_write`, `split_output`, `content_type`, `writes_directory`, `wants_local_file`, `create(options)`.
- `FormatReader`: `infer_schema`, `infer_stats`, `open`, `write_ordering`, `write`.
- `WriteRequest`: `path`, `schema`, `local_path`. It has no options. The writer gets its options only from `create(options)` as flat `String` keys.

The Beacon host (`beacon-plugin-formats/src/lib.rs`) says: "Not supported yet: statistics, the crawler, and `COPY ... TO`."
So Beacon does not call a plugin writer yet. The owner wants the writer in the plugin now, ready for that host support.

Beacon refuses a plugin format when its name, its extensions or its functions are already in use.
Built-in Beacon does not register `odv` or `txt` as a `STORED AS` format (`register_file_formats` in `beacon-core/src/runtime_builder.rs`).
The built-in code gives only `read_odv_ascii`, the JSON query `from: odv` and the ODV query output.
So the plugin can install on the current Beacon, next to the built-in code.

Beacon `main` has version `2.1.0-rc.1`. The template range `>=2.1, <3` can refuse a pre-release.
Beacon `main` has no release tags.

## Decisions by the owner

- The plugin format name is `odv`. Users write `STORED AS ODV`.
- The plugin reads and writes ODV.
- The work changes only this repo.

## Decisions by Claude (the owner can change these)

- **Plugin name.** `beacon-plugin.toml` gets `name = "odv"`. The native crate gets the name `odv-native`, library `odv_native`.
- **Example content.** Remove all example functions, `example_tsv`, `sql/functions.sql` and the example test data. Keep the build and release scripts.
- **Table functions.** The SDK makes `read_odv(...)` and `read_odv_schema(...)` from the format name. The old name `read_odv_ascii` goes away. The owner notes this in the Beacon change later.
- **Extensions.** `extensions()` returns only `txt`, as today. Beacon then also accepts `STORED AS TXT` for ODV. Today Beacon names ODV output `.zip`. The README records that the later host write support must name ODV output `.zip`.
- **Beacon version.** Set the `beacon` range in `beacon-plugin.toml` so that the current `main` (`2.1.0-rc.1`) accepts the plugin. Check how Beacon compares versions first. Pin `beacon-plugin-sdk` to the commit `a612fce`, or a later one.
- **Compressed files.** Keep the read support for `.gz`, `.bz2`, `.xz`, `.zst` and `.zstd`, if the SDK `open` call gives the object path. If a codec crate conflicts with the plugin build, drop that codec and record it in the README.
- **Output kind.** `OutputKind::Flat`.
- **Write options.** The table option `odv_options` holds `OdvOptions` as JSON text. Without this option, the writer calls `OdvOptions::try_from_arrow_schema` on the input schema, as the Python `to_odv` does today.
- **Write order.** `write_ordering` returns the sort keys key column, time column, depth column, all ascending, as `create_writer_physical_plan` does today.
- **Write output.** `content_type` is `application/zip`. `split_output` is `false`, because one ODV archive holds all rows.
- **Copied code.** Copy `reader.rs` and `writer.rs` with their tests. Replace each `beacon-common` and `beacon-datafusion-ext` use with SDK types or a local copy of the small helper.
- **README.** Rewrite it for the ODV plugin: install, `STORED AS ODV`, `read_odv`, write options, and the two host limits above.

## Tests

- Keep the unit tests of the copied reader and writer.
- Add tests on the SDK interface, in the style of `native/src/tsv.rs`: `infer_schema`, `open` with a projection and a batch size, and `write` with and without `odv_options`.
- Add `test_file.txt` and `test_file.txt.zst` from Beacon as test data. Test that both give the same schema and rows.
- Add a parity test for the reader: the plugin `open` gives the same schema and rows as the copied `AsyncOdvDecoder` on the test file.
- Add a write test: read the test file, write it with explicit `odv_options` that match its column names, and open the zip. Check the zip entry names and the number of data rows. No full round trip, because the reader cannot read the zip and the writer can round values.
- Keep the `#[ignore]` tests of `reader.rs` as they are.
- `cargo test` and `cargo clippy` in `native/` must pass on Windows and Linux.
- No live test in a running Beacon. The owner decided that unit tests are enough.

## Left to the builder

- The module layout inside `native/src`.
- How to adapt the DataFusion `FileSource` and `DataSink` code to `FormatReader::open` and `FormatReader::write`.
- `writes_directory` and `wants_local_file`, from what the writer needs.
- The pin of `beacon-plugin-sdk` in `native/Cargo.toml`, to a tag or commit that matches the DataFusion version of the copied code.
