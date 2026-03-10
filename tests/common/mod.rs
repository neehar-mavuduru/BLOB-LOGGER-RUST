use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use blob_logger_rust::buffer::HEADER_OFFSET;
use blob_logger_rust::config::Config;
use blob_logger_rust::logger::Logger;

/// Creates a default Config pointing at `base_dir`.
pub fn make_config(base_dir: &Path) -> Config {
    Config {
        num_shards: 4,
        buffer_size: 4 * 65536,
        max_file_size: 1024 * 1024,
        log_file_path: base_dir.to_path_buf(),
        flush_interval: Duration::from_secs(300),
        gcs_config: None,
    }
}

/// Creates a Logger backed by real files in a temp dir.
#[allow(dead_code)]
pub fn make_logger(base_dir: &Path) -> Logger {
    let logs_dir = base_dir.join("logs");
    let upload_ready_dir = base_dir.join("upload_ready");
    std::fs::create_dir_all(&logs_dir).expect("create logs dir");
    std::fs::create_dir_all(&upload_ready_dir).expect("create upload_ready dir");

    let mut config = make_config(base_dir);
    config.validate().expect("config validation");

    Logger::new("test_event", &logs_dir, &upload_ready_dir, config).expect("create logger")
}

/// Decodes all length-prefixed records from a block-formatted log file.
#[allow(dead_code)]
pub fn read_all_records(path: &Path) -> Vec<Vec<u8>> {
    let data = std::fs::read(path).expect("read log file");
    let mut records = Vec::new();
    let mut pos = 0;

    while pos + 8 <= data.len() {
        let block_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let valid_offset = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;

        if block_size == 0 {
            break;
        }

        let payload_start = pos + HEADER_OFFSET;
        let payload_end = payload_start + valid_offset;
        let payload_end = payload_end.min(data.len());

        let mut rpos = payload_start;
        while rpos + 4 <= payload_end {
            let rec_len =
                u32::from_le_bytes(data[rpos..rpos + 4].try_into().unwrap()) as usize;
            if rpos + 4 + rec_len > payload_end {
                break;
            }
            records.push(data[rpos + 4..rpos + 4 + rec_len].to_vec());
            rpos += 4 + rec_len;
        }

        pos += block_size;
    }

    records
}

/// Finds all `.log` files in a directory.
#[allow(dead_code)]
pub fn find_log_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read dir")
        .flatten()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "log")
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    files.sort();
    files
}

/// Finds all symlinks in a directory.
#[allow(dead_code)]
pub fn find_symlinks(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read dir")
        .flatten()
        .filter(|e| {
            std::fs::symlink_metadata(e.path())
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    files.sort();
    files
}

/// Asserts no `.tmp` files exist in a directory.
#[allow(dead_code)]
pub fn assert_no_tmp_files(dir: &Path) {
    let tmp_files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read dir")
        .flatten()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "tmp")
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();

    assert!(
        tmp_files.is_empty(),
        "found .tmp files: {:?}",
        tmp_files
    );
}

/// Decodes records from a raw byte slice (same format as log files).
#[allow(dead_code)]
pub fn decode_records(data: &[u8]) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    let mut pos = 0;

    while pos + 8 <= data.len() {
        let block_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let valid_offset = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;

        if block_size == 0 {
            break;
        }

        let payload_start = pos + HEADER_OFFSET;
        let payload_end = (payload_start + valid_offset).min(data.len());

        let mut rpos = payload_start;
        while rpos + 4 <= payload_end {
            let rec_len =
                u32::from_le_bytes(data[rpos..rpos + 4].try_into().unwrap()) as usize;
            if rpos + 4 + rec_len > payload_end {
                break;
            }
            records.push(data[rpos + 4..rpos + 4 + rec_len].to_vec());
            rpos += 4 + rec_len;
        }

        pos += block_size;
    }

    records
}

// ─── FakeObjectStore ────────────────────────────────────────────────────────

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::FutureExt;
use object_store::{
    path::Path as ObjPath, Attributes, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload,
    PutResult, Result as ObjResult, UploadPart,
};

/// In-memory tracking state for the fake object store.
#[derive(Debug, Default)]
pub struct FakeStoreState {
    pub objects: HashMap<String, Bytes>,
    pub multipart_calls: Vec<String>,
    pub abort_calls: Vec<String>,
    pub put_call_count: usize,
}

/// In-memory implementation of `ObjectStore` for testing.
#[derive(Debug)]
pub struct FakeObjectStore {
    pub state: Arc<parking_lot::Mutex<FakeStoreState>>,
}

impl FakeObjectStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            state: Arc::new(parking_lot::Mutex::new(FakeStoreState::default())),
        }
    }
}

impl std::fmt::Display for FakeObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FakeObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FakeObjectStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjResult<PutResult> {
        let mut all_bytes = Vec::new();
        for chunk in payload.into_iter() {
            all_bytes.extend_from_slice(&chunk);
        }
        let data = Bytes::from(all_bytes);

        let mut state = self.state.lock();
        state.put_call_count += 1;
        state.objects.insert(location.to_string(), data);
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        _opts: PutMultipartOpts,
    ) -> ObjResult<Box<dyn MultipartUpload>> {
        let state = self.state.clone();
        state.lock().multipart_calls.push(location.to_string());
        Ok(Box::new(FakeMultipartUpload {
            location: location.to_string(),
            state,
            buffer: Vec::new(),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        _opts: GetOptions,
    ) -> ObjResult<GetResult> {
        let state = self.state.lock();
        let data = state
            .objects
            .get(&location.to_string())
            .cloned()
            .ok_or_else(|| object_store::Error::NotFound {
                path: location.to_string(),
                source: "not found in FakeObjectStore".into(),
            })?;

        let len = data.len();
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(
                futures::stream::once(async move { Ok(data) }),
            )),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size: len,
                e_tag: None,
                version: None,
            },
            range: 0..len,
            attributes: Attributes::default(),
        })
    }

    async fn delete(&self, location: &ObjPath) -> ObjResult<()> {
        self.state.lock().objects.remove(&location.to_string());
        Ok(())
    }

    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, ObjResult<ObjectMeta>> {
        let state = self.state.lock();
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let items: Vec<ObjResult<ObjectMeta>> = state
            .objects
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix_str))
            .map(|(k, v)| {
                Ok(ObjectMeta {
                    location: ObjPath::from(k.as_str()),
                    last_modified: chrono::Utc::now(),
                    size: v.len(),
                    e_tag: None,
                    version: None,
                })
            })
            .collect();
        Box::pin(futures::stream::iter(items))
    }

    async fn list_with_delimiter(&self, _prefix: Option<&ObjPath>) -> ObjResult<ListResult> {
        Ok(ListResult {
            common_prefixes: vec![],
            objects: vec![],
        })
    }

    async fn copy(&self, from: &ObjPath, to: &ObjPath) -> ObjResult<()> {
        let data = {
            let state = self.state.lock();
            state
                .objects
                .get(&from.to_string())
                .cloned()
                .ok_or_else(|| object_store::Error::NotFound {
                    path: from.to_string(),
                    source: "not found".into(),
                })?
        };
        self.state.lock().objects.insert(to.to_string(), data);
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> ObjResult<()> {
        self.copy(from, to).await
    }
}

/// Fake multipart upload that buffers data in memory.
#[derive(Debug)]
struct FakeMultipartUpload {
    location: String,
    state: Arc<parking_lot::Mutex<FakeStoreState>>,
    buffer: Vec<u8>,
}

#[async_trait::async_trait]
impl MultipartUpload for FakeMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        for chunk in data.into_iter() {
            self.buffer.extend_from_slice(&chunk);
        }
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> ObjResult<PutResult> {
        let data = Bytes::from(std::mem::take(&mut self.buffer));
        self.state
            .lock()
            .objects
            .insert(self.location.clone(), data);
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> ObjResult<()> {
        self.state
            .lock()
            .abort_calls
            .push(self.location.clone());
        self.buffer.clear();
        Ok(())
    }
}
