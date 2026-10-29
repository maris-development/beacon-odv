# ODV Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn this template repo into a Beacon plugin with the name `odv` that reads and writes ODV spreadsheet files.

**Architecture:** Copy the ODV reader and writer from Beacon into `native/src`. Wrap them in the plugin SDK traits `FormatFactory` and `FormatReader`. Drop the DataFusion `FileFormat`, `FileSource`, `DataSink` and table function code of Beacon, because the SDK host gives these.

**Tech Stack:** Rust 2024, `beacon-plugin-sdk` (DataFusion 54, Arrow 58, object_store 0.13), tokio, zip, async_zip.

**Spec:** `docs/superpowers/specs/2026-10-29-odv-plugin-design.md`

**Source:** Beacon at commit `a612fce`. Get it with:

```bash
git clone https://github.com/maris-development/beacon ../beacon-src && git -C ../beacon-src checkout a612fce
```

The ODV crate is `../beacon-src/beacon-db/beacon-file-formats/beacon-arrow-odv`. The SDK is `../beacon-src/beacon-db/beacon-plugin-sdk`.

## Global Constraints

- The format name is `odv`. `extensions()` returns only `["txt"]`.
- Plugin manifest: `name = "odv"`, `version = "0.1.0"`, `beacon = ">=2.1.0-rc.1, <3"`.
- Native crate: package `odv-native`, library `odv_native`, `crate-type = ["cdylib"]`.
- Pin `beacon-plugin-sdk` with `rev = "<full hash>"`, not `branch = "main"`. Get the hash with `git -C ../beacon-src rev-parse a612fce`.
- Use Arrow, DataFusion and object_store only through `beacon_plugin_sdk::datafusion` and `beacon_plugin_sdk::object_store`. `cargo tree -d` in `native/` must show no second `arrow-*` or `datafusion` version. If `arrow-csv` is not in the re-export, add it at the exact version that `Cargo.lock` already holds for `arrow`.
- Remove the `utoipa::ToSchema` derives. Keep the `serde` derives and the serde field names and aliases unchanged.
- No `unwrap()` or `expect()` on I/O, parse or user-option results in non-test code. Map each error to `DataFusionError`.
- The reader must give the same schema and rows as Beacon gives today. The writer must give the same files.
- No change to the Beacon repo. No live Beacon test.
- Code comments and docs follow ASD-STE100: active voice, simple present, one thought per sentence.

## Review Focus

1. A table option `odv_options` with invalid JSON: `create` returns an error that names `odv_options`. No panic.
2. `odv_options` names a column that the input does not have: `write` returns an error. No panic, no partial object left in the store.
3. A `ScanRequest` with a subset of columns in a different order: `open` returns batches with these columns, in this order, by name.
4. An ODV file with a header and no data rows: `infer_schema` gives the schema, `open` gives zero rows. No error.
5. A `.txt` file that is not ODV (for example plain CSV): `infer_schema` returns an error. No panic.

Each item has a test in the task that owns the code.

---

### Task 1: Turn the template into the ODV plugin crate

**Files:**
- Modify: `beacon-plugin.toml`, `native/Cargo.toml`, `native/src/lib.rs`, `.gitignore` (only if it names `example`)
- Delete: `native/src/tsv.rs`, `native/testdata/stations.etsv`, `sql/functions.sql`, the `[sql]` table of `beacon-plugin.toml`
- Create: `native/src/reader.rs`, `native/src/writer.rs`, `native/src/error.rs` (copies from the ODV crate `src/`)
- Create: `native/testdata/test_file.txt`, `native/testdata/test_file.txt.zst` (copies of `../beacon-src/test-datasets/test_file.txt*`)

**Interfaces:**
- Produces: `crate::reader::{AsyncOdvDecoder, OdvSchemaMapper}`, `crate::writer::{AsyncOdvWriter, OdvOptions, ColumnInfo}`, `crate::error::OdvError`, `crate::OdvResult<T>`, with the signatures of the Beacon source. Important ones:
  - `AsyncOdvDecoder::decode_schema_mapper<S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin>(stream: S) -> Result<OdvSchemaMapper, ArrowError>`
  - `AsyncOdvDecoder::decode<S>(stream: S, projection: Option<Vec<usize>>, mapper: Arc<OdvSchemaMapper>) -> impl Stream<Item = Result<RecordBatch, ArrowError>>`
  - `OdvSchemaMapper::output_schema(&self) -> SchemaRef`
  - `OdvOptions::try_from_arrow_schema(schema: SchemaRef) -> anyhow::Result<OdvOptions>`
  - `AsyncOdvWriter::new_from_dyn(writer: Box<W>, schema: SchemaRef, options: OdvOptions) -> anyhow::Result<AsyncOdvWriter<W>>`, `write(&mut self, batch: RecordBatch) -> anyhow::Result<()>`, `finish(self) -> anyhow::Result<W>`

- [ ] **Step 1: Copy `reader.rs`, `writer.rs`, `error.rs` with their `#[cfg(test)]` modules.** Do not copy `lib.rs` or `datafusion/`. Point the test paths of the copied tests at `native/testdata/`. Keep the two `#[ignore]` tests of `reader.rs` as they are.

- [ ] **Step 2: Fix imports.** Replace `arrow::`, `datafusion::` and `object_store::` paths with the SDK re-exports. Remove `utoipa`. Add the other crates to `native/Cargo.toml` at the Beacon workspace versions: `serde` 1 (derive), `serde_json` 1, `tokio` 1.47 (`rt`, `io-util`, `fs`, `macros`), `tokio-util` 0.7 (`io`, `compat`), `indexmap` 2.7, `anyhow` 1, `thiserror` 2, `tracing` 0.1, `tempfile` 3, `csv` 1.3, `regex` 1.11, `walkdir` 2.5, `bytes` 1.10, `zip` 2.2.3 (`default-features = false`, `deflate`), `async_zip` 0.0.18 (`deflate`, `tokio`, `zstd`). Drop each crate that the copied code does not use.

- [ ] **Step 3: Rewrite `native/src/lib.rs`.** It holds `mod reader; mod writer; mod error; mod format;`, `pub type OdvResult<T>`, and `beacon_plugin_sdk::export_plugin!(formats = format::formats);`. Create `native/src/format.rs` with `pub fn formats() -> Vec<Arc<dyn FormatFactory>>` that returns an empty `Vec` for now. Update `beacon-plugin.toml` and `native/Cargo.toml` per Global Constraints.

- [ ] **Step 4: Run the copied tests.**

Run: `cargo test --manifest-path native/Cargo.toml`
Expected: PASS. The two reader tests show as ignored. Then run `cargo tree -d --manifest-path native/Cargo.toml` and check for no second `arrow-*` or `datafusion` version.

- [ ] **Step 5: Commit**

```bash
git add -A beacon-plugin.toml native sql
git commit -m "Replace template example with ODV reader and writer"
```

### Task 2: Read ODV through the SDK

**Files:**
- Modify: `native/src/format.rs`
- Test: `native/src/format.rs` (`#[cfg(test)] mod tests`, in the style of the removed `tsv.rs` tests: `InMemory` store, `ObjectMeta` from a `put`)

**Interfaces:**
- Consumes: Task 1 decoder API.
- Produces:
  - `pub struct OdvFormat;` with `impl FormatFactory`: `name() = "odv"`, `extensions() = ["txt"]`, `output_kind() = OutputKind::Flat`, `table_function_args() = []`, `create(options) -> Result<Arc<dyn FormatReader>>`.
  - `pub struct OdvReader { options: Option<OdvOptions> }` with `impl FormatReader`: `infer_schema`, `open`. (Task 3 adds the write methods and the `options` value.)
  - `fn compression_of(location: &Path) -> FileCompressionType`: `gz` to GZIP, `bz2` to BZIP2, `xz` to XZ, `zst` and `zstd` to ZSTD, else UNCOMPRESSED. Same map as `OdvFormat::infer_compression` in Beacon `datafusion/mod.rs`.
  - `async fn byte_stream(store, object) -> Result<impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin>`: `store.get`, then `compression_of(..).convert_stream(..)`.

- [ ] **Step 1: Write the failing tests.**

```rust
#[tokio::test] async fn factory_describes_odv()
// name() == "odv"; extensions() == vec!["txt"]; output_kind() == OutputKind::Flat

#[tokio::test] async fn infer_schema_matches_the_decoder()
// schema from OdvReader::infer_schema on testdata/test_file.txt
// == AsyncOdvDecoder::decode_schema_mapper(..).output_schema() on the same bytes

#[tokio::test] async fn open_gives_the_same_rows_as_the_decoder()
// ScanRequest { schema: full schema, filters: [], limit: None, batch_size: 8192 }
// concat of open() batches == concat of AsyncOdvDecoder::decode(.., None, ..) batches; 895 rows

#[tokio::test] async fn zst_file_gives_the_same_schema_and_rows()
// testdata/test_file.txt.zst stored as "test_file.txt.zst": schema and rows == plain file

#[tokio::test] async fn open_gives_the_requested_columns_in_order()   // Review Focus 3
// request schema = [Station, Cruise] (reverse of file order)
// every batch: schema().fields() names == ["Station", "Cruise"]; total rows 895

#[tokio::test] async fn header_without_rows_gives_schema_and_no_rows() // Review Focus 4
// bytes = the `//` lines and the header line of test_file.txt only
// infer_schema Ok with the same field names as the full file; open gives 0 rows

#[tokio::test] async fn csv_text_is_an_error()                          // Review Focus 5
// bytes = b"a,b\n1,2\n" stored as "plain.txt"; infer_schema is Err, no panic
```

- [ ] **Step 2: Run the tests.**

Run: `cargo test --manifest-path native/Cargo.toml format::tests`
Expected: FAIL. `OdvFormat` and `OdvReader` do not exist.

- [ ] **Step 3: Implement `OdvFormat`, `OdvReader::infer_schema`, `OdvReader::open`, `compression_of`, `byte_stream`.** In `open`, decode the schema mapper first. Then map `request.schema` field names to indices of `mapper.output_schema()`, and pass these as the projection to `decode`. A requested name that the file does not have is left out (the host fills it). Wrap the stream with `RecordBatchStreamAdapter` on the projected schema. Map `ArrowError` to `DataFusionError::ArrowError`. Return `formats()` as `vec![Arc::new(OdvFormat)]`. If a codec of `FileCompressionType` is not in the SDK DataFusion build, drop that extension from `compression_of` and note it for the README in Task 4.

- [ ] **Step 4: Run the tests.**

Run: `cargo test --manifest-path native/Cargo.toml`
Expected: PASS, all tests. If `csv_text_is_an_error` fails because the decoder accepts CSV and gives a schema, stop and report. Do not change the decoder, because the spec forbids changes to read results.

- [ ] **Step 5: Commit**

```bash
git add native/src/format.rs native/src/lib.rs
git commit -m "Read ODV files through the plugin SDK"
```

### Task 3: Write ODV through the SDK

**Files:**
- Modify: `native/src/format.rs`

**Interfaces:**
- Consumes: Task 1 writer API, Task 2 `OdvFormat`, `OdvReader`.
- Produces:
  - `OdvFormat`: `can_write() = true`, `split_output() = false`, `content_type() = "application/zip"`, `writes_directory() = false`, `wants_local_file() = false`.
  - `create(options)`: reads the key `odv_options`. Present: `serde_json::from_str::<OdvOptions>`. Error message: `invalid odv_options: <serde error>`. Absent: `options = None`.
  - `OdvReader::write_ordering(schema)`: with `options = Some(o)`: `[o.key_column, o.time_column.column_name, o.depth_column.column_name]`. With `None`: the same three names from `OdvOptions::try_from_arrow_schema(schema)`. Each `SortKey { descending: false, nulls_first: false }`. Drop a key whose column is not in `schema`.
  - `OdvReader::write(store, request, input) -> Result<u64>`: options from `self.options`, or from `try_from_arrow_schema(request.schema)`. Output target: `tokio::fs::File` at `request.local_path` when set. Else `object_store::buffered::BufWriter::new(store, request.path)`. Feed every batch to `AsyncOdvWriter::write`. Call `finish`, then flush and shut down the target. Return the row count.

- [ ] **Step 1: Write the failing tests.**

```rust
#[test] fn factory_can_write_zip()
// can_write() true; split_output() false; content_type() == "application/zip"

#[test] fn invalid_odv_options_is_an_error()                 // Review Focus 1
// create({"odv_options": "{not json"}) is Err; error text contains "odv_options"

#[tokio::test] async fn write_ordering_uses_key_time_depth()
// options with key "Cruise", time "yyyy-mm-ddThh:mm:ss.sss", depth "Depth":
// write_ordering(test file schema) columns == ["Cruise", "yyyy-mm-ddThh:mm:ss.sss", "Depth"], all ascending

#[tokio::test] async fn write_with_options_gives_a_zip_with_all_rows()
// read test_file.txt with open(); build OdvOptions from the inferred schema:
//   key = "Cruise"; longitude, latitude, depth = the field whose name starts with "Longitude", "Latitude", "Depth";
//   time = the field whose name starts with "yyyy"; data_columns = the other numeric fields
// serialize to JSON, create({"odv_options": json}), sort the batches by write_ordering, write to InMemory "out.zip"
// write() returns 895
// the stored object opens as a zip; each entry name ends with ".txt"
// the data lines (not "//", not the header line) over all entries sum to 895

#[tokio::test] async fn write_without_options_infers_them()
// same input, create({}); assert only that write() returns (Ok or Err) and does not panic.
// Put the actual result (row count or error text) in the task report.

#[tokio::test] async fn unknown_option_column_is_an_error()   // Review Focus 2
// odv_options with key_column "NoSuchColumn": write() is Err; store.head("out.zip") is Err (no object)
```

- [ ] **Step 2: Run the tests.**

Run: `cargo test --manifest-path native/Cargo.toml format::tests`
Expected: FAIL. `can_write` is `false` and `write` is not implemented.

- [ ] **Step 3: Implement the Task 3 interfaces in `native/src/format.rs`.** For Review Focus 2, check that each option column exists in `request.schema` before the target opens. For an error after the target opens, call `BufWriter::abort` (or delete `request.path`) before the error returns.

- [ ] **Step 4: Run the tests.**

Run: `cargo test --manifest-path native/Cargo.toml`
Expected: PASS. If the zip data lines do not sum to 895, stop and report the numbers. Do not weaken the test and do not change the writer.

- [ ] **Step 5: Commit**

```bash
git add native/src/format.rs native/Cargo.toml native/Cargo.lock
git commit -m "Write ODV archives through the plugin SDK"
```

### Task 4: README, lint and release check

**Files:**
- Modify: `README.md`, `.github/workflows/release.yml` (only if it names `example`), `scripts/package-plugin.py` (only if it names `example`)

**Interfaces:**
- Consumes: Tasks 1 to 3.

- [ ] **Step 1: Rewrite `README.md`.** Sections: what the plugin does; install (`INSTALL odv FROM 'github:<owner>/beacon-odv@v0.1.0'` and `BEACON_PLUGINS=odv=...`); read (`CREATE EXTERNAL TABLE ... STORED AS ODV LOCATION '...'`, `read_odv(...)`, `read_odv_schema(...)`, the compressed extensions); write options (`odv_options` JSON with every `OdvOptions` field and its default); limits: Beacon does not call plugin writers yet (`COPY ... TO`, query output), output must use `.zip` when it does, `STORED AS TXT` also selects ODV, `read_odv_ascii` of Beacon becomes `read_odv`. ASD-STE100 style.

- [ ] **Step 2: Run lint and tests.**

Run: `cargo clippy --manifest-path native/Cargo.toml --all-targets -- -D warnings` and `cargo test --manifest-path native/Cargo.toml`
Expected: no warnings, all tests PASS.

- [ ] **Step 3: Run the package script.**

Run: `cargo build --release --manifest-path native/Cargo.toml` and then `python scripts/package-plugin.py --library native/target/release/odv_native.dll --target x86_64-pc-windows-msvc` (see `--help` for the output flag)
Expected: an archive named for `odv` and `0.1.0`. No error about the manifest.

- [ ] **Step 4: Commit**

```bash
git add README.md .github scripts
git commit -m "Document the ODV plugin"
```
