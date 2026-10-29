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
use beacon_plugin_sdk::file_format::{FormatFactory, FormatReader, OutputKind, ScanRequest};
use beacon_plugin_sdk::object_store::store::path::Path;
use beacon_plugin_sdk::object_store::store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::reader::{AsyncOdvDecoder, OdvSchemaMapper};

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

    fn create(&self, _options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        Ok(Arc::new(OdvReader))
    }
}

/// A configured `odv` format.
#[derive(Debug)]
pub struct OdvReader;

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

async fn schema_mapper(store: &Arc<dyn ObjectStore>, object: &ObjectMeta) -> Result<OdvSchemaMapper> {
    Ok(AsyncOdvDecoder::decode_schema_mapper(byte_stream(store, object).await?).await?)
}

#[async_trait]
impl FormatReader for OdvReader {
    async fn infer_schema(&self, store: Arc<dyn ObjectStore>, object: &ObjectMeta) -> Result<SchemaRef> {
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
    use beacon_plugin_sdk::datafusion::arrow::compute::concat_batches;
    use beacon_plugin_sdk::datafusion::arrow::datatypes::{Schema, SchemaRef};
    use beacon_plugin_sdk::file_format::{FormatFactory, FormatReader, OutputKind, ScanRequest};
    use beacon_plugin_sdk::object_store::store::memory::InMemory;
    use beacon_plugin_sdk::object_store::store::path::Path;
    use beacon_plugin_sdk::object_store::store::{ObjectMeta, ObjectStore, ObjectStoreExt};
    use bytes::Bytes;
    use futures::TryStreamExt;

    use super::OdvFormat;
    use crate::reader::AsyncOdvDecoder;

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

    fn one_chunk(bytes: &[u8]) -> futures::stream::Iter<std::vec::IntoIter<std::io::Result<Bytes>>> {
        futures::stream::iter(vec![Ok(Bytes::copy_from_slice(bytes))])
    }

    /// The schema and all rows that the copied decoder gives for `bytes`.
    async fn decoded(bytes: &[u8]) -> (SchemaRef, RecordBatch) {
        let mapper = AsyncOdvDecoder::decode_schema_mapper(one_chunk(bytes)).await.unwrap();
        let schema = mapper.output_schema();
        let batches: Vec<RecordBatch> = AsyncOdvDecoder::decode(one_chunk(bytes), None, Arc::new(mapper))
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
            let names: Vec<&str> = batch.schema_ref().fields().iter().map(|f| f.name().as_str()).collect();
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
        let names = |s: &Schema| s.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();
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
}
