//! The `odv` file format of the plugin SDK.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use beacon_plugin_sdk::datafusion::arrow::datatypes::SchemaRef;
use beacon_plugin_sdk::datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use beacon_plugin_sdk::datafusion::error::{DataFusionError, Result};
use beacon_plugin_sdk::datafusion::execution::SendableRecordBatchStream;
use beacon_plugin_sdk::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use beacon_plugin_sdk::file_format::{
    FormatFactory, FormatReader, OutputKind, ScanRequest, SortKey, WriteRequest,
};
use beacon_plugin_sdk::object_store::store::buffered::BufWriter;
use beacon_plugin_sdk::object_store::store::path::Path;
use beacon_plugin_sdk::object_store::store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use bytes::Bytes;
use futures::{AsyncWrite, AsyncWriteExt, Stream, StreamExt, TryStreamExt};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::reader::{AsyncOdvDecoder, OdvSchemaMapper};
use crate::writer::{AsyncOdvWriter, OdvOptions};

/// The table option with the write options, as `OdvOptions` JSON.
const ODV_OPTIONS: &str = "odv_options";

/// The `odv` format.
///
/// Beacon adds `STORED AS odv`, `read_odv` and `read_odv_schema`.
#[derive(Debug)]
pub struct OdvFormat;

impl FormatFactory for OdvFormat {
    fn name(&self) -> &str {
        "odv"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["txt".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Flat
    }

    fn can_write(&self) -> bool {
        true
    }

    /// One ODV archive holds all rows.
    fn split_output(&self) -> bool {
        false
    }

    fn content_type(&self) -> &str {
        "application/zip"
    }

    fn create(&self, options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        let options = options
            .get(ODV_OPTIONS)
            .map(|json| serde_json::from_str::<OdvOptions>(json))
            .transpose()
            .map_err(|e| DataFusionError::Configuration(format!("invalid {ODV_OPTIONS}: {e}")))?;
        Ok(Arc::new(OdvReader { options }))
    }
}

/// A configured `odv` format.
#[derive(Debug)]
pub struct OdvReader {
    /// The write options. Without them, the writer derives them from the input schema.
    options: Option<OdvOptions>,
}

impl OdvReader {
    fn write_options(&self, schema: &SchemaRef) -> Result<OdvOptions> {
        match &self.options {
            Some(options) => Ok(options.clone()),
            None => OdvOptions::try_from_arrow_schema(schema.clone()).map_err(|e| {
                DataFusionError::Execution(format!("cannot derive the ODV write options: {e}"))
            }),
        }
    }
}

fn write_error(error: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(format!("cannot write ODV: {error:#}"))
}

/// The compression of a file, from the extension of its path.
fn compression_of(location: &Path) -> FileCompressionType {
    match location.extension() {
        Some("gz") => FileCompressionType::GZIP,
        Some("bz2") => FileCompressionType::BZIP2,
        Some("xz") => FileCompressionType::XZ,
        Some("zst") | Some("zstd") => FileCompressionType::ZSTD,
        _ => FileCompressionType::UNCOMPRESSED,
    }
}

/// The uncompressed bytes of one file.
async fn byte_stream(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<impl Stream<Item = std::io::Result<Bytes>> + Unpin + use<>> {
    let stream = store
        .get(&object.location)
        .await?
        .into_stream()
        .map_err(DataFusionError::from)
        .boxed();
    let stream = compression_of(&object.location).convert_stream(stream)?;
    Ok(stream.map_err(std::io::Error::other))
}

async fn schema_mapper(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<OdvSchemaMapper> {
    Ok(AsyncOdvDecoder::decode_schema_mapper(byte_stream(store, object).await?).await?)
}

#[async_trait]
impl FormatReader for OdvReader {
    async fn infer_schema(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> Result<SchemaRef> {
        Ok(schema_mapper(&store, object).await?.output_schema())
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let mapper = schema_mapper(&store, object).await?;
        let file_schema = mapper.output_schema();
        // Beacon fills a requested column that this file does not have with nulls.
        let projection: Vec<usize> = request
            .schema
            .fields()
            .iter()
            .filter_map(|field| file_schema.index_of(field.name()).ok())
            .collect();
        let schema = Arc::new(file_schema.project(&projection)?);
        let body = byte_stream(&store, object).await?;
        let batches = if projection.is_empty() {
            // `count(*)` asks for no columns, so keep only the row count.
            let empty = Arc::clone(&schema);
            AsyncOdvDecoder::decode(body, None, Arc::new(mapper))
                .await
                .map(move |batch| {
                    let options = RecordBatchOptions::new().with_row_count(Some(batch?.num_rows()));
                    RecordBatch::try_new_with_options(Arc::clone(&empty), vec![], &options)
                })
                .boxed()
        } else {
            AsyncOdvDecoder::decode(body, Some(projection), Arc::new(mapper))
                .await
                .boxed()
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batches.map_err(DataFusionError::from),
        )))
    }

    fn write_ordering(&self, schema: SchemaRef) -> Result<Vec<SortKey>> {
        let options = self.write_options(&schema)?;
        let columns = [
            options.key_column,
            options.time_column.column_name,
            options.depth_column.column_name,
        ];
        Ok(columns
            .into_iter()
            .filter(|column| schema.index_of(column).is_ok())
            .map(|column| SortKey {
                column,
                descending: false,
                nulls_first: false,
            })
            .collect())
    }

    async fn write(
        &self,
        store: Arc<dyn ObjectStore>,
        request: WriteRequest,
        mut input: SendableRecordBatchStream,
    ) -> Result<u64> {
        let options = self.write_options(&request.schema)?;
        // The writer puts nothing in the target before `finish`, so an error leaves no object.
        let target: Box<dyn AsyncWrite + Unpin + Send> = match &request.local_path {
            Some(path) => Box::new(tokio::fs::File::create(path).await?.compat_write()),
            None => Box::new(BufWriter::new(store, request.path.clone()).compat_write()),
        };
        let mut writer = AsyncOdvWriter::new_from_dyn(target, request.schema.clone(), options)
            .await
            .map_err(write_error)?;
        let mut rows = 0;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            rows += batch.num_rows() as u64;
            writer.write(batch).await.map_err(write_error)?;
        }
        let mut target = writer.finish().await.map_err(write_error)?;
        target.close().await?;
        Ok(rows)
    }
}

/// The file formats of this plugin.
pub fn formats() -> Vec<Arc<dyn FormatFactory>> {
    vec![Arc::new(OdvFormat)]
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use beacon_plugin_sdk::datafusion::arrow::array::RecordBatch;
    use beacon_plugin_sdk::datafusion::arrow::compute::{
        concat_batches, lexsort_to_indices, take_record_batch, SortColumn, SortOptions,
    };
    use beacon_plugin_sdk::datafusion::arrow::datatypes::{Schema, SchemaRef};
    use beacon_plugin_sdk::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use beacon_plugin_sdk::file_format::{
        FormatFactory, FormatReader, OutputKind, ScanRequest, WriteRequest,
    };
    use beacon_plugin_sdk::object_store::store::memory::InMemory;
    use beacon_plugin_sdk::object_store::store::path::Path;
    use beacon_plugin_sdk::object_store::store::{ObjectMeta, ObjectStore, ObjectStoreExt};
    use bytes::Bytes;
    use futures::TryStreamExt;

    use super::OdvFormat;
    use crate::reader::AsyncOdvDecoder;
    use crate::writer::{ColumnInfo, OdvOptions};

    const TEST_FILE: &[u8] = include_bytes!("../testdata/test_file.txt");
    const TEST_FILE_ZST: &[u8] = include_bytes!("../testdata/test_file.txt.zst");
    const TEST_FILE_ROWS: usize = 895;

    /// Puts `bytes` at `name` in an in-memory store.
    async fn stored(name: &str, bytes: &[u8]) -> (Arc<dyn ObjectStore>, ObjectMeta) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from(name);
        store.put(&path, bytes.to_vec().into()).await.unwrap();
        let object = store.head(&path).await.unwrap();
        (store, object)
    }

    fn reader() -> Arc<dyn FormatReader> {
        OdvFormat.create(&HashMap::new()).unwrap()
    }

    fn one_chunk(
        bytes: &[u8],
    ) -> futures::stream::Iter<std::vec::IntoIter<std::io::Result<Bytes>>> {
        futures::stream::iter(vec![Ok(Bytes::copy_from_slice(bytes))])
    }

    /// The schema and all rows that the copied decoder gives for `bytes`.
    async fn decoded(bytes: &[u8]) -> (SchemaRef, RecordBatch) {
        let mapper = AsyncOdvDecoder::decode_schema_mapper(one_chunk(bytes))
            .await
            .unwrap();
        let schema = mapper.output_schema();
        let batches: Vec<RecordBatch> =
            AsyncOdvDecoder::decode(one_chunk(bytes), None, Arc::new(mapper))
                .await
                .try_collect()
                .await
                .unwrap();
        let all = concat_batches(&schema, &batches).unwrap();
        (schema, all)
    }

    fn request(schema: SchemaRef) -> ScanRequest {
        ScanRequest {
            schema,
            filters: vec![],
            limit: None,
            batch_size: 8192,
        }
    }

    /// All batches of `open`, and their count of rows.
    async fn opened(name: &str, bytes: &[u8], schema: SchemaRef) -> Vec<RecordBatch> {
        let (store, object) = stored(name, bytes).await;
        reader()
            .open(store, &object, request(schema))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[tokio::test]
    async fn factory_describes_odv() {
        assert_eq!(OdvFormat.name(), "odv");
        assert_eq!(OdvFormat.extensions(), vec!["txt".to_string()]);
        assert!(matches!(OdvFormat.output_kind(), OutputKind::Flat));
    }

    #[tokio::test]
    async fn infer_schema_matches_the_decoder() {
        let (store, object) = stored("test_file.txt", TEST_FILE).await;
        let schema = reader().infer_schema(store, &object).await.unwrap();
        let (expected, _) = decoded(TEST_FILE).await;
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn open_gives_the_same_rows_as_the_decoder() {
        let (schema, expected) = decoded(TEST_FILE).await;
        let batches = opened("test_file.txt", TEST_FILE, schema.clone()).await;
        let all = concat_batches(&schema, &batches).unwrap();
        assert_eq!(all, expected);
        assert_eq!(all.num_rows(), TEST_FILE_ROWS);
    }

    #[tokio::test]
    async fn zst_file_gives_the_same_schema_and_rows() {
        let (store, object) = stored("test_file.txt.zst", TEST_FILE_ZST).await;
        let schema = reader().infer_schema(store, &object).await.unwrap();
        let (expected_schema, expected) = decoded(TEST_FILE).await;
        assert_eq!(schema, expected_schema);
        let batches = opened("test_file.txt.zst", TEST_FILE_ZST, schema.clone()).await;
        assert_eq!(concat_batches(&schema, &batches).unwrap(), expected);
    }

    #[tokio::test]
    async fn open_gives_the_requested_columns_in_order() {
        let (schema, _) = decoded(TEST_FILE).await;
        let wanted = Schema::new(vec![
            schema.field_with_name("Station").unwrap().clone(),
            schema.field_with_name("Cruise").unwrap().clone(),
        ]);
        let batches = opened("test_file.txt", TEST_FILE, Arc::new(wanted)).await;
        for batch in &batches {
            let names: Vec<&str> = batch
                .schema_ref()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            assert_eq!(names, ["Station", "Cruise"]);
        }
        assert_eq!(rows(&batches), TEST_FILE_ROWS);
    }

    #[tokio::test]
    async fn no_requested_columns_still_counts_the_rows() {
        let batches = opened("test_file.txt", TEST_FILE, Arc::new(Schema::empty())).await;
        assert_eq!(rows(&batches), TEST_FILE_ROWS);
    }

    #[tokio::test]
    async fn header_without_rows_gives_schema_and_no_rows() {
        let text = std::str::from_utf8(TEST_FILE).unwrap();
        let header_only: String = text
            .lines()
            .take_while(|line| line.starts_with("//"))
            .chain(text.lines().find(|line| !line.starts_with("//")))
            .map(|line| format!("{line}\n"))
            .collect();
        let (store, object) = stored("empty.txt", header_only.as_bytes()).await;
        let schema = reader().infer_schema(store, &object).await.unwrap();
        let (full, _) = decoded(TEST_FILE).await;
        let names = |s: &Schema| {
            s.fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&schema), names(&full));
        let batches = opened("empty.txt", header_only.as_bytes(), schema).await;
        assert_eq!(rows(&batches), 0);
    }

    #[tokio::test]
    async fn empty_file_is_an_error() {
        let (store, object) = stored("empty.txt", b"").await;
        assert!(reader().infer_schema(store, &object).await.is_err());
    }

    #[tokio::test]
    async fn csv_text_does_not_panic() {
        let (store, object) = stored("plain.txt", b"a,b\n1,2\n").await;
        let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
            reader().infer_schema(store, &object),
        ))
        .await;
        assert!(result.is_ok(), "infer_schema panicked");
    }

    fn column(name: &str) -> ColumnInfo {
        ColumnInfo {
            column_name: name.to_string(),
            comment: None,
            significant_digits: None,
            qf_column: None,
            units: None,
        }
    }

    /// The field of `schema` with the name `prefix`, else the first one that starts with it.
    fn named(schema: &Schema, prefix: &str) -> String {
        let fields = schema.fields();
        fields
            .iter()
            .find(|f| f.name() == prefix)
            .or_else(|| fields.iter().find(|f| f.name().starts_with(prefix)))
            .unwrap()
            .name()
            .clone()
    }

    /// Write options that match the columns of the test file.
    fn test_file_options(schema: &Schema) -> OdvOptions {
        let core = ["Cruise", "Longitude", "Latitude", "Depth", "yyyy"]
            .map(|prefix| named(schema, prefix));
        let data_columns = schema
            .fields()
            .iter()
            .filter(|f| f.data_type().is_numeric() && !core.contains(f.name()))
            .map(|f| column(f.name()))
            .collect();
        OdvOptions {
            key_column: core[0].clone(),
            longitude_column: column(&core[1]),
            latitude_column: column(&core[2]),
            depth_column: column(&core[3]),
            time_column: column(&core[4]),
            data_columns,
            meta_columns: vec![],
            ..OdvOptions::default()
        }
    }

    fn with_options(options: &OdvOptions) -> HashMap<String, String> {
        HashMap::from([(
            "odv_options".to_string(),
            serde_json::to_string(options).unwrap(),
        )])
    }

    /// All rows of the test file, sorted as `writer` asks.
    async fn sorted_test_file(writer: &Arc<dyn FormatReader>) -> (SchemaRef, RecordBatch) {
        let (schema, all) = decoded(TEST_FILE).await;
        let keys = writer.write_ordering(schema.clone()).unwrap();
        let columns: Vec<SortColumn> = keys
            .iter()
            .map(|key| SortColumn {
                values: all.column_by_name(&key.column).unwrap().clone(),
                options: Some(SortOptions {
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                }),
            })
            .collect();
        let indices = lexsort_to_indices(&columns, None).unwrap();
        let sorted = take_record_batch(&all, &indices).unwrap();
        (schema, sorted)
    }

    async fn write(
        writer: &Arc<dyn FormatReader>,
        store: &Arc<dyn ObjectStore>,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> beacon_plugin_sdk::datafusion::error::Result<u64> {
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch)]),
        ));
        let request = WriteRequest {
            path: Path::from("out.zip"),
            schema,
            local_path: None,
        };
        writer.write(Arc::clone(store), request, input).await
    }

    #[test]
    fn factory_can_write_zip() {
        assert!(OdvFormat.can_write());
        assert!(!OdvFormat.split_output());
        assert_eq!(OdvFormat.content_type(), "application/zip");
    }

    #[test]
    fn invalid_odv_options_is_an_error() {
        let options = HashMap::from([("odv_options".to_string(), "{not json".to_string())]);
        let error = OdvFormat.create(&options).err().expect("an error");
        assert!(error.to_string().contains("odv_options"), "{error}");
    }

    #[tokio::test]
    async fn write_ordering_uses_key_time_depth() {
        let (schema, _) = decoded(TEST_FILE).await;
        let writer = OdvFormat
            .create(&with_options(&test_file_options(&schema)))
            .unwrap();
        let keys = writer.write_ordering(schema).unwrap();
        let columns: Vec<&str> = keys.iter().map(|k| k.column.as_str()).collect();
        assert_eq!(columns, ["Cruise", "yyyy-mm-ddThh:mm:ss.sss", "Depth"]);
        assert!(keys.iter().all(|k| !k.descending));
    }

    #[tokio::test]
    async fn write_with_options_gives_a_zip_with_all_rows() {
        let (schema, _) = decoded(TEST_FILE).await;
        let writer = OdvFormat
            .create(&with_options(&test_file_options(&schema)))
            .unwrap();
        let (schema, sorted) = sorted_test_file(&writer).await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let written = write(&writer, &store, schema, sorted).await.unwrap();
        assert_eq!(written, TEST_FILE_ROWS as u64);

        let archive = store
            .get(&Path::from("out.zip"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(archive.to_vec())).unwrap();
        let mut data_lines = 0;
        for index in 0..zip.len() {
            let mut entry = zip.by_index(index).unwrap();
            assert!(entry.name().ends_with(".txt"), "{}", entry.name());
            let mut text = String::new();
            std::io::Read::read_to_string(&mut entry, &mut text).unwrap();
            // One header line, then one line per row.
            let lines = text
                .lines()
                .filter(|l| !l.starts_with("//") && !l.is_empty())
                .count();
            data_lines += lines.saturating_sub(1);
        }
        assert_eq!(data_lines, TEST_FILE_ROWS);
    }

    #[tokio::test]
    async fn write_without_options_does_not_panic() {
        let writer = OdvFormat.create(&HashMap::new()).unwrap();
        let (schema, all) = decoded(TEST_FILE).await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(write(
            &writer, &store, schema, all,
        )))
        .await;
        let result = result.expect("write panicked");
        println!("write without odv_options: {result:?}");
    }

    #[tokio::test]
    async fn unknown_option_column_is_an_error() {
        let (schema, all) = decoded(TEST_FILE).await;
        let options = OdvOptions {
            key_column: "NoSuchColumn".to_string(),
            ..test_file_options(&schema)
        };
        let writer = OdvFormat.create(&with_options(&options)).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(write(&writer, &store, schema, all).await.is_err());
        assert!(store.head(&Path::from("out.zip")).await.is_err());
    }
}
