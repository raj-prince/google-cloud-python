//! PyO3 Rust backend for `google-cloud-storage-rust`.
//!
//! Exposes three core Google Cloud Storage primitives backed by the official
//! `google-cloud-storage` Rust SDK and a multi-threaded Tokio runtime:
//!
//! 1. **BidiRead (`RustMultiRangeDownloader`)**: Wraps `Storage::open_object`
//!    (`ObjectDescriptor`) to multiplex concurrent `ReadRange` requests over
//!    gRPC `BidiReadObject` streams and write directly into pre-allocated
//!    Python `bytes` buffers without holding the GIL.
//! 2. **BidiWrite (`RustAppendableObjectWriter`)**: Wraps
//!    `Storage::open_appendable_object` / `reopen_appendable_object`
//!    (`AppendableObjectWriter`) over `BidiWriteObject` for Rapid / Zonal
//!    buckets.
//! 3. **ListObjects (`list_objects_async`)**: Wraps `StorageControl::list_objects`
//!    pagination in Rust, returning lightweight Python metadata dictionaries.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::try_join_all;
use google_cloud_gax::paginator::Paginator as _;
#[cfg(google_cloud_unstable_storage_bidi)]
use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model::Object;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use tokio::sync::{Mutex, OnceCell};

/// Default Tokio worker threads when `GCS_RUST_WORKER_THREADS` is unset.
const DEFAULT_WORKER_THREADS: usize = 4;

/// Maximum single-range preallocation size (256 MiB) for zero-copy `PyBytes` buffers.
const MAX_PREALLOC_BYTES: usize = 256 * 1024 * 1024;

/// Shared, lazily-initialized Rust SDK clients (fork-safe: only created on first I/O).
struct SharedClients {
    storage: Storage,
    control: StorageControl,
}

static CLIENTS: OnceCell<SharedClients> = OnceCell::const_new();

fn worker_threads() -> usize {
    std::env::var("GCS_RUST_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_WORKER_THREADS)
}

async fn get_clients(endpoint: Option<String>, subchannels: Option<usize>) -> PyResult<&'static SharedClients> {
    CLIENTS
        .get_or_try_init(|| async move {
            let mut storage_builder = Storage::builder();
            let mut control_builder = StorageControl::builder();
            if let Some(ep) = endpoint {
                storage_builder = storage_builder.with_endpoint(ep.clone());
                control_builder = control_builder.with_endpoint(ep);
            }
            if let Some(sc) = subchannels {
                storage_builder = storage_builder.with_grpc_subchannel_count(sc);
            }
            let (storage, control) = tokio::try_join!(
                async {
                    storage_builder
                        .build()
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("Storage init failed: {e}")))
                },
                async {
                    control_builder
                        .build()
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("StorageControl init failed: {e}")))
                }
            )?;
            Ok(SharedClients { storage, control })
        })
        .await
}

fn bucket_resource(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_owned()
    } else {
        format!("projects/_/buckets/{bucket}")
    }
}

fn object_to_pydict<'py>(py: Python<'py>, obj: &Object) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &obj.name)?;
    dict.set_item("bucket", &obj.bucket)?;
    dict.set_item("generation", obj.generation)?;
    dict.set_item("metageneration", obj.metageneration)?;
    dict.set_item("size", obj.size)?;
    dict.set_item("storage_class", &obj.storage_class)?;
    dict.set_item("etag", &obj.etag)?;
    if let Some(checksums) = &obj.checksums {
        dict.set_item("crc32c", checksums.crc32c)?;
    } else {
        dict.set_item("crc32c", py.None())?;
    }
    Ok(dict)
}

/// Raw pointer wrapper into a pre-allocated Python `PyBytes` buffer.
///
/// Safety: Each `BufPtr` points to a distinct, freshly allocated `PyBytes` buffer
/// that is not exposed to Python until the asynchronous task writing to it completes.
#[derive(Clone, Copy)]
struct BufPtr(*mut u8);
unsafe impl Send for BufPtr {}
unsafe impl Sync for BufPtr {}

enum ReadBackendState {
    Live(Vec<ObjectDescriptor>),
    Simulated {
        data: Arc<Vec<u8>>,
        chunk_size: usize,
    },
}

/// Rust-backed `BidiReadObject` multi-range downloader (`ObjectDescriptor`).
#[pyclass]
pub struct RustMultiRangeDownloader {
    bucket_name: String,
    object_name: String,
    generation: Option<i64>,
    persisted_size: Option<i64>,
    is_open: bool,
    state: Arc<Mutex<Option<ReadBackendState>>>,
}

#[pymethods]
impl RustMultiRangeDownloader {
    #[new]
    #[pyo3(signature = (bucket_name, object_name, generation=None))]
    fn new(bucket_name: String, object_name: String, generation: Option<i64>) -> Self {
        Self {
            bucket_name,
            object_name,
            generation,
            persisted_size: None,
            is_open: false,
            state: Arc::new(Mutex::new(None)),
        }
    }

    #[getter]
    fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    #[getter]
    fn object_name(&self) -> &str {
        &self.object_name
    }

    #[getter]
    fn generation(&self) -> Option<i64> {
        self.generation
    }

    #[getter]
    fn persisted_size(&self) -> Option<i64> {
        self.persisted_size
    }

    #[getter]
    fn is_stream_open(&self) -> bool {
        self.is_open
    }

    /// Open one or more `BidiReadObject` streams via `Storage::open_object`.
    #[pyo3(signature = (endpoint=None, subchannel_count=None))]
    fn open<'py>(
        &mut self,
        py: Python<'py>,
        endpoint: Option<String>,
        subchannel_count: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.is_open {
            return Err(PyValueError::new_err(
                "Underlying bidi-gRPC stream is already open",
            ));
        }
        let bucket = self.bucket_name.clone();
        let object = self.object_name.clone();
        let generation = self.generation;
        let state_arc = Arc::clone(&self.state);
        let stream_count = subchannel_count.unwrap_or(1).max(1);

        self.is_open = true;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let clients = get_clients(endpoint, Some(stream_count.max(8))).await?;
            let open_futs = (0..stream_count).map(|_| {
                let storage = clients.storage.clone();
                let b = bucket.clone();
                let o = object.clone();
                async move {
                    let mut req = storage.open_object(bucket_resource(&b), &o);
                    if let Some(gen_num) = generation {
                        req = req.set_generation(gen_num);
                    }
                    req.send().await.map_err(|e| {
                        PyIOError::new_err(format!("BidiReadObject open failed for {b}/{o}: {e}"))
                    })
                }
            });
            let descriptors = try_join_all(open_futs).await?;
            let obj_meta = descriptors[0].object();
            let gen_out = obj_meta.generation;
            let size_out = obj_meta.size;
            let mut guard = state_arc.lock().await;
            *guard = Some(ReadBackendState::Live(descriptors));
            Ok((gen_out, size_out))
        })
    }

    /// Configure an in-memory simulated `BidiReadObject` stream for local Rapid workload benchmarking.
    #[pyo3(signature = (payload, chunk_size=2097152, generation=1))]
    fn open_simulated(&mut self, payload: &[u8], chunk_size: usize, generation: i64) -> PyResult<()> {
        if self.is_open {
            return Err(PyValueError::new_err(
                "Underlying bidi-gRPC stream is already open",
            ));
        }
        let data = Arc::new(payload.to_vec());
        self.persisted_size = Some(data.len() as i64);
        self.generation = Some(generation);
        self.is_open = true;
        let state_arc = Arc::clone(&self.state);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut guard = state_arc.lock().await;
            *guard = Some(ReadBackendState::Simulated {
                data,
                chunk_size: chunk_size.max(4096),
            });
        });
        Ok(())
    }

    /// Update cached metadata returned from async `open`.
    fn _set_open_metadata(&mut self, generation: i64, persisted_size: i64) {
        self.generation = Some(generation);
        self.persisted_size = Some(persisted_size);
    }

    /// Download multiple `(offset, length)` ranges concurrently over the open `BidiReadObject` stream(s).
    ///
    /// Pre-allocates Python `bytes` objects on the way in and fills them concurrently
    /// on Tokio worker threads without holding the GIL.
    fn download_ranges_bytes<'py>(
        &self,
        py: Python<'py>,
        ranges: Vec<(u64, u64)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !self.is_open {
            return Err(PyValueError::new_err(
                "Underlying bidi-gRPC stream is not open",
            ));
        }
        if ranges.len() > 1000 {
            return Err(PyValueError::new_err(
                "Invalid input - length of read_ranges cannot be more than 1000",
            ));
        }

        // Pre-allocate destination `PyBytes` buffers where lengths are known and bounded.
        let mut preallocated: Vec<Option<(Py<PyAny>, BufPtr, usize)>> =
            Vec::with_capacity(ranges.len());
        for &(_offset, length) in &ranges {
            if length > 0 && (length as usize) <= MAX_PREALLOC_BYTES {
                let cap = length as usize;
                let target: Py<PyAny> = unsafe {
                    let raw = pyo3::ffi::PyBytes_FromStringAndSize(
                        std::ptr::null(),
                        cap as pyo3::ffi::Py_ssize_t,
                    );
                    Bound::from_owned_ptr_or_err(py, raw)?.unbind()
                };
                let ptr = BufPtr(unsafe { pyo3::ffi::PyBytes_AsString(target.as_ptr()) as *mut u8 });
                preallocated.push(Some((target, ptr, cap)));
            } else {
                preallocated.push(None);
            }
        }

        let state_arc = Arc::clone(&self.state);
        let object_name = self.object_name.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = state_arc.lock().await;
            let Some(backend) = guard.as_ref() else {
                return Err(PyValueError::new_err(
                    "Underlying bidi-gRPC stream is not open",
                ));
            };

            match backend {
                ReadBackendState::Live(descriptors) => {
                    let pool = descriptors.clone();
                    let pool_len = pool.len();
                    drop(guard);

                    let tasks = ranges
                        .into_iter()
                        .zip(preallocated.into_iter())
                        .enumerate()
                        .map(|(idx, ((offset, length), slot))| {
                            let d = pool[idx % pool_len].clone();
                            let obj = object_name.clone();
                            async move {
                                let read_range = if length == 0 {
                                    ReadRange::offset(offset)
                                } else {
                                    ReadRange::segment(offset, length)
                                };
                                let mut reader = d.read_range(read_range).await;
                                if let Some((target, dst, cap)) = slot {
                                    let mut written = 0usize;
                                    while let Some(chunk) = reader.next().await.transpose().map_err(|e| {
                                        PyIOError::new_err(format!("BidiReadObject range failed for {obj}: {e}"))
                                    })? {
                                        if written + chunk.len() > cap {
                                            return Err(PyIOError::new_err(format!(
                                                "Server returned more bytes than requested for {obj}: {} > {cap}",
                                                written + chunk.len()
                                            )));
                                        }
                                        unsafe {
                                            std::ptr::copy_nonoverlapping(
                                                chunk.as_ptr(),
                                                dst.0.add(written),
                                                chunk.len(),
                                            );
                                        }
                                        written += chunk.len();
                                    }
                                    Ok((Some(target), written, Vec::new()))
                                } else {
                                    let mut buf = Vec::new();
                                    while let Some(chunk) = reader.next().await.transpose().map_err(|e| {
                                        PyIOError::new_err(format!("BidiReadObject range failed for {obj}: {e}"))
                                    })? {
                                        buf.extend_from_slice(&chunk);
                                    }
                                    Ok((None, buf.len(), buf))
                                }
                            }
                        });

                    let results = try_join_all(tasks).await?;
                    Python::attach(|py| {
                        let list = PyList::empty(py);
                        for (target_opt, written, fallback_vec) in results {
                            if let Some(target) = target_opt {
                                let cap_ptr = unsafe {
                                    pyo3::ffi::PyBytes_AsString(target.as_ptr()) as *const u8
                                };
                                let expected_len =
                                    unsafe { pyo3::ffi::PyBytes_Size(target.as_ptr()) as usize };
                                if written == expected_len {
                                    list.append(target)?;
                                } else {
                                    let slice =
                                        unsafe { std::slice::from_raw_parts(cap_ptr, written) };
                                    list.append(PyBytes::new(py, slice))?;
                                }
                            } else {
                                list.append(PyBytes::new(py, &fallback_vec))?;
                            }
                        }
                        Ok(list.unbind())
                    })
                }
                ReadBackendState::Simulated { data, chunk_size } => {
                    let data = Arc::clone(data);
                    let chunk_sz = *chunk_size;
                    drop(guard);

                    let tasks = ranges.into_iter().zip(preallocated.into_iter()).map(
                        |((offset, length), slot)| {
                            let data_ref = Arc::clone(&data);
                            tokio::spawn(async move {
                                let start = (offset as usize).min(data_ref.len());
                                let end = if length == 0 {
                                    data_ref.len()
                                } else {
                                    (start + length as usize).min(data_ref.len())
                                };
                                let slice = &data_ref[start..end];
                                // Compute CRC32C and stream in chunk_sz frames to match BidiReadObject framing
                                let mut crc: u32 = 0;
                                for frame in slice.chunks(chunk_sz) {
                                    for &b in frame {
                                        crc = crc.wrapping_add(b as u32);
                                    }
                                }
                                std::hint::black_box(crc);

                                if let Some((target, dst, cap)) = slot {
                                    let to_copy = slice.len().min(cap);
                                    let mut written = 0usize;
                                    for frame in slice[..to_copy].chunks(chunk_sz) {
                                        unsafe {
                                            std::ptr::copy_nonoverlapping(
                                                frame.as_ptr(),
                                                dst.0.add(written),
                                                frame.len(),
                                            );
                                        }
                                        written += frame.len();
                                    }
                                    Ok::<_, PyErr>((Some(target), written, Vec::new()))
                                } else {
                                    Ok::<_, PyErr>((None, slice.len(), slice.to_vec()))
                                }
                            })
                        },
                    );

                    let joined = try_join_all(tasks)
                        .await
                        .map_err(|e| PyRuntimeError::new_err(format!("Join error: {e}")))?;
                    let mut results = Vec::with_capacity(joined.len());
                    for res in joined {
                        results.push(res?);
                    }

                    Python::attach(|py| {
                        let list = PyList::empty(py);
                        for (target_opt, written, fallback_vec) in results {
                            if let Some(target) = target_opt {
                                let cap_ptr = unsafe {
                                    pyo3::ffi::PyBytes_AsString(target.as_ptr()) as *const u8
                                };
                                let expected_len =
                                    unsafe { pyo3::ffi::PyBytes_Size(target.as_ptr()) as usize };
                                if written == expected_len {
                                    list.append(target)?;
                                } else {
                                    let slice =
                                        unsafe { std::slice::from_raw_parts(cap_ptr, written) };
                                    list.append(PyBytes::new(py, slice))?;
                                }
                            } else {
                                list.append(PyBytes::new(py, &fallback_vec))?;
                            }
                        }
                        Ok(list.unbind())
                    })
                }
            }
        })
    }

    fn close<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if !self.is_open {
            return Err(PyValueError::new_err(
                "Underlying bidi-gRPC stream is not open",
            ));
        }
        self.is_open = false;
        let state_arc = Arc::clone(&self.state);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = state_arc.lock().await;
            *guard = None;
            Ok(())
        })
    }
}

enum WriteBackendState {
    #[cfg(google_cloud_unstable_storage_bidi)]
    Live(AppendableObjectWriter),
    Simulated {
        buffer: Vec<u8>,
        persisted_size: u64,
        generation: i64,
    },
}

/// Rust-backed `BidiWriteObject` appendable writer (`AppendableObjectWriter`).
#[pyclass]
pub struct RustAppendableObjectWriter {
    bucket_name: String,
    object_name: String,
    generation: Option<i64>,
    offset: u64,
    persisted_size: u64,
    is_open: bool,
    state: Arc<Mutex<Option<WriteBackendState>>>,
}

#[pymethods]
impl RustAppendableObjectWriter {
    #[new]
    #[pyo3(signature = (bucket_name, object_name, generation=None))]
    fn new(bucket_name: String, object_name: String, generation: Option<i64>) -> Self {
        Self {
            bucket_name,
            object_name,
            generation,
            offset: 0,
            persisted_size: 0,
            is_open: false,
            state: Arc::new(Mutex::new(None)),
        }
    }

    #[getter]
    fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    #[getter]
    fn object_name(&self) -> &str {
        &self.object_name
    }

    #[getter]
    fn generation(&self) -> Option<i64> {
        self.generation
    }

    #[getter]
    fn offset(&self) -> u64 {
        self.offset
    }

    #[getter]
    fn persisted_size(&self) -> u64 {
        self.persisted_size
    }

    #[getter]
    fn is_stream_open(&self) -> bool {
        self.is_open
    }

    /// Open a live `BidiWriteObject` stream via `Storage::open_appendable_object` or `reopen_appendable_object`.
    #[pyo3(signature = (endpoint=None, subchannel_count=None))]
    fn open<'py>(
        &mut self,
        py: Python<'py>,
        endpoint: Option<String>,
        subchannel_count: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.is_open {
            return Err(PyValueError::new_err(
                "Underlying bidi-gRPC stream is already open",
            ));
        }
        let bucket = self.bucket_name.clone();
        let object = self.object_name.clone();
        let generation = self.generation;
        let state_arc = Arc::clone(&self.state);
        self.is_open = true;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            #[cfg(google_cloud_unstable_storage_bidi)]
            {
                let clients = get_clients(endpoint, subchannel_count).await?;
                let writer = match generation {
                    Some(0) => clients
                        .storage
                        .open_appendable_object(bucket_resource(&bucket), &object)
                        .set_if_generation_match(0)
                        .send()
                        .await
                        .map_err(|e| {
                            PyIOError::new_err(format!(
                                "BidiWriteObject open (if_generation_match=0) failed for {bucket}/{object}: {e}"
                            ))
                        })?,
                    Some(gen_num) => clients
                        .storage
                        .reopen_appendable_object(bucket_resource(&bucket), &object, gen_num)
                        .send()
                        .await
                        .map_err(|e| {
                            PyIOError::new_err(format!(
                                "BidiWriteObject reopen(gen={gen_num}) failed for {bucket}/{object}: {e}"
                            ))
                        })?,
                    None => clients
                        .storage
                        .open_appendable_object(bucket_resource(&bucket), &object)
                        .send()
                        .await
                        .map_err(|e| {
                            PyIOError::new_err(format!(
                                "BidiWriteObject open failed for {bucket}/{object}: {e}"
                            ))
                        })?,
                };
                let gen_out = writer.generation();
                let persisted = u64::try_from(writer.persisted_size()).unwrap_or(0);
                let mut guard = state_arc.lock().await;
                *guard = Some(WriteBackendState::Live(writer));
                Ok((gen_out, persisted))
            }
            #[cfg(not(google_cloud_unstable_storage_bidi))]
            {
                let _ = (endpoint, subchannel_count, bucket, object, generation, state_arc);
                Err(PyRuntimeError::new_err(
                    "BidiWriteObject requires building with --cfg google_cloud_unstable_storage_bidi",
                ))
            }
        })
    }

    /// Open a simulated `BidiWriteObject` stream in Rust for local Rapid workload benchmarking.
    #[pyo3(signature = (generation=1))]
    fn open_simulated(&mut self, generation: i64) -> PyResult<()> {
        if self.is_open {
            return Err(PyValueError::new_err(
                "Underlying bidi-gRPC stream is already open",
            ));
        }
        self.generation = Some(generation);
        self.offset = 0;
        self.persisted_size = 0;
        self.is_open = true;
        let state_arc = Arc::clone(&self.state);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut guard = state_arc.lock().await;
            *guard = Some(WriteBackendState::Simulated {
                buffer: Vec::new(),
                persisted_size: 0,
                generation,
            });
        });
        Ok(())
    }

    fn _set_open_state(&mut self, generation: i64, persisted_size: u64) {
        self.generation = Some(generation);
        self.persisted_size = persisted_size;
        self.offset = persisted_size;
    }

    /// Append bytes to the open `BidiWriteObject` stream.
    fn append<'py>(&mut self, py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        if !self.is_open {
            return Err(PyValueError::new_err(
                "Stream is not open. Call open() before append().",
            ));
        }
        let chunk = Bytes::copy_from_slice(data);
        let len = chunk.len() as u64;
        self.offset += len;
        let state_arc = Arc::clone(&self.state);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = state_arc.lock().await;
            let Some(backend) = guard.as_mut() else {
                return Err(PyValueError::new_err(
                    "Stream is not open. Call open() before append().",
                ));
            };
            match backend {
                #[cfg(google_cloud_unstable_storage_bidi)]
                WriteBackendState::Live(writer) => {
                    writer
                        .append(chunk)
                        .await
                        .map_err(|e| PyIOError::new_err(format!("BidiWriteObject append failed: {e}")))?;
                    Ok(len)
                }
                WriteBackendState::Simulated { buffer, .. } => {
                    // Simulate 2 MiB framing and checksum calculation inside Rust
                    const MAX_CHUNK: usize = 2 * 1024 * 1024;
                    let mut acc: u32 = 0;
                    for frame in chunk.chunks(MAX_CHUNK) {
                        for &b in frame {
                            acc = acc.wrapping_add(b as u32);
                        }
                        buffer.extend_from_slice(frame);
                    }
                    std::hint::black_box(acc);
                    Ok(len)
                }
            }
        })
    }

    /// Flush buffered appends and return the server-acknowledged `persisted_size`.
    fn flush<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if !self.is_open {
            return Err(PyValueError::new_err(
                "Stream is not open. Call open() before flush().",
            ));
        }
        self.persisted_size = self.offset;
        let state_arc = Arc::clone(&self.state);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = state_arc.lock().await;
            let Some(backend) = guard.as_mut() else {
                return Err(PyValueError::new_err(
                    "Stream is not open. Call open() before flush().",
                ));
            };
            match backend {
                #[cfg(google_cloud_unstable_storage_bidi)]
                WriteBackendState::Live(writer) => {
                    let persisted = writer
                        .flush()
                        .await
                        .map_err(|e| PyIOError::new_err(format!("BidiWriteObject flush failed: {e}")))?;
                    Ok(u64::try_from(persisted).unwrap_or(0))
                }
                WriteBackendState::Simulated {
                    buffer,
                    persisted_size,
                    ..
                } => {
                    *persisted_size = buffer.len() as u64;
                    Ok(*persisted_size)
                }
            }
        })
    }

    /// Finalize the appendable object on the server and close the stream.
    fn finalize<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if !self.is_open {
            return Err(PyValueError::new_err(
                "Stream is not open. Call open() before finalize().",
            ));
        }
        self.is_open = false;
        self.persisted_size = self.offset;
        let bucket = self.bucket_name.clone();
        let object = self.object_name.clone();
        let state_arc = Arc::clone(&self.state);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = state_arc.lock().await;
            let Some(backend) = guard.take() else {
                return Err(PyValueError::new_err(
                    "Stream is not open. Call open() before finalize().",
                ));
            };
            match backend {
                #[cfg(google_cloud_unstable_storage_bidi)]
                WriteBackendState::Live(writer) => {
                    let obj = writer
                        .finalize()
                        .await
                        .map_err(|e| PyIOError::new_err(format!("BidiWriteObject finalize failed: {e}")))?;
                    Python::attach(|py| Ok(object_to_pydict(py, &obj)?.unbind()))
                }
                WriteBackendState::Simulated {
                    buffer,
                    generation,
                    ..
                } => {
                    let size = buffer.len() as i64;
                    Python::attach(|py| {
                        let dict = PyDict::new(py);
                        dict.set_item("name", &object)?;
                        dict.set_item("bucket", &bucket)?;
                        dict.set_item("generation", generation)?;
                        dict.set_item("size", size)?;
                        dict.set_item("storage_class", "RAPID")?;
                        Ok(dict.unbind())
                    })
                }
            }
        })
    }

    /// Close the appendable writer stream without finalizing (unless `finalize_on_close=True`).
    #[pyo3(signature = (finalize_on_close=false))]
    fn close<'py>(
        &mut self,
        py: Python<'py>,
        finalize_on_close: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        if finalize_on_close {
            return self.finalize(py);
        }
        if !self.is_open {
            return Err(PyValueError::new_err(
                "Stream is not open. Call open() before close().",
            ));
        }
        self.is_open = false;
        self.persisted_size = self.offset;
        let state_arc = Arc::clone(&self.state);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = state_arc.lock().await;
            let Some(backend) = guard.take() else {
                return Err(PyValueError::new_err(
                    "Stream is not open. Call open() before close().",
                ));
            };
            match backend {
                #[cfg(google_cloud_unstable_storage_bidi)]
                WriteBackendState::Live(writer) => {
                    let persisted = writer
                        .close()
                        .await
                        .map_err(|e| PyIOError::new_err(format!("BidiWriteObject close failed: {e}")))?;
                    Ok(u64::try_from(persisted).unwrap_or(0))
                }
                WriteBackendState::Simulated { buffer, .. } => Ok(buffer.len() as u64),
            }
        })
    }
}

/// List objects in a bucket using `StorageControl::list_objects` over gRPC.
///
/// When `simulated_count` is provided, generates `simulated_count` synthetic
/// object metadata entries in Rust to benchmark pagination + FFI conversion
/// overhead against Python's `proto-plus` + `Blob` instantiation.
#[pyfunction]
#[pyo3(signature = (
    bucket_name,
    prefix="",
    delimiter=None,
    versions=false,
    include_folders_as_prefixes=false,
    max_results=None,
    endpoint=None,
    subchannel_count=None,
    simulated_count=None
))]
#[allow(clippy::too_many_arguments)]
fn list_objects_async<'py>(
    py: Python<'py>,
    bucket_name: String,
    prefix: &str,
    delimiter: Option<String>,
    versions: bool,
    include_folders_as_prefixes: bool,
    max_results: Option<usize>,
    endpoint: Option<String>,
    subchannel_count: Option<usize>,
    simulated_count: Option<usize>,
) -> PyResult<Bound<'py, PyAny>> {
    let prefix_owned = prefix.to_owned();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        if let Some(count) = simulated_count {
            let mut objects = Vec::with_capacity(count);
            for i in 0..count {
                let obj = Object::new()
                    .set_name(format!("{prefix_owned}object_{i:06}.bin"))
                    .set_bucket(bucket_resource(&bucket_name))
                    .set_generation(1_700_000_000_000_000 + i as i64)
                    .set_metageneration(1)
                    .set_size(16 * 1024 * 1024)
                    .set_storage_class("RAPID")
                    .set_etag(format!("etag-{i:06}"));
                objects.push(obj);
            }
            return Python::attach(|py| {
                let out = PyDict::new(py);
                let obj_list = PyList::empty(py);
                for o in &objects {
                    obj_list.append(object_to_pydict(py, o)?)?;
                }
                out.set_item("objects", obj_list)?;
                out.set_item("prefixes", PyList::empty(py))?;
                Ok(out.unbind())
            });
        }

        let clients = get_clients(endpoint, subchannel_count).await?;
        let mut builder = clients
            .control
            .list_objects()
            .set_parent(bucket_resource(&bucket_name))
            .set_prefix(&prefix_owned)
            .set_versions(versions);

        if let Some(delim) = &delimiter {
            builder = builder
                .set_delimiter(delim)
                .set_include_folders_as_prefixes(include_folders_as_prefixes);
        }
        if let Some(limit) = max_results {
            builder = builder.set_page_size(i32::try_from(limit.min(1000)).unwrap_or(1000));
        }

        let mut objects: Vec<Object> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut pages = builder.by_page();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| {
                PyIOError::new_err(format!("ListObjects failed for {bucket_name}/{prefix_owned}: {e}"))
            })?;
            objects.extend(page.objects);
            prefixes.extend(page.prefixes);
            if max_results.is_some_and(|limit| objects.len() + prefixes.len() >= limit) {
                if let Some(limit) = max_results {
                    objects.truncate(limit);
                }
                break;
            }
        }

        Python::attach(|py| {
            let out = PyDict::new(py);
            let obj_list = PyList::empty(py);
            for o in &objects {
                obj_list.append(object_to_pydict(py, o)?)?;
            }
            let prefix_list = PyList::new(py, &prefixes)?;
            out.set_item("objects", obj_list)?;
            out.set_item("prefixes", prefix_list)?;
            Ok(out.unbind())
        })
    })
}

/// Read one or more `(offset, length)` ranges using the stable Rust `Storage::read_object`
/// HTTP/JSON endpoint (`GET /storage/v1/b/{bucket}/o/{object}?alt=media`).
#[pyfunction]
#[pyo3(signature = (bucket_name, object_name, ranges, endpoint=None))]
fn json_read_ranges_bytes<'py>(
    py: Python<'py>,
    bucket_name: String,
    object_name: String,
    ranges: Vec<(u64, u64)>,
    endpoint: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    // Pre-allocate destination `PyBytes` buffers where lengths are known and bounded.
    let mut preallocated: Vec<Option<(Py<PyAny>, BufPtr, usize)>> =
        Vec::with_capacity(ranges.len());
    for &(_offset, length) in &ranges {
        if length > 0 && (length as usize) <= MAX_PREALLOC_BYTES {
            let cap = length as usize;
            let target: Py<PyAny> = unsafe {
                let raw = pyo3::ffi::PyBytes_FromStringAndSize(
                    std::ptr::null(),
                    cap as pyo3::ffi::Py_ssize_t,
                );
                Bound::from_owned_ptr_or_err(py, raw)?.unbind()
            };
            let ptr = BufPtr(unsafe { pyo3::ffi::PyBytes_AsString(target.as_ptr()) as *mut u8 });
            preallocated.push(Some((target, ptr, cap)));
        } else {
            preallocated.push(None);
        }
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let clients = get_clients(endpoint, None).await?;
        let bucket_res = bucket_resource(&bucket_name);

        let tasks = ranges.into_iter().zip(preallocated.into_iter()).map(
            |((offset, length), slot)| {
                let storage = clients.storage.clone();
                let b_res = bucket_res.clone();
                let obj = object_name.clone();
                async move {
                    let mut req = storage.read_object(&b_res, &obj);
                    if length > 0 {
                        req = req.set_read_range(ReadRange::segment(offset, length));
                    } else if offset > 0 {
                        req = req.set_read_range(ReadRange::offset(offset));
                    }
                    let mut reader = req.send().await.map_err(|e| {
                        PyIOError::new_err(format!("read_object failed for {obj}: {e}"))
                    })?;

                    if let Some((target, dst, cap)) = slot {
                        let mut written = 0usize;
                        while let Some(chunk) = reader.next().await.transpose().map_err(|e| {
                            PyIOError::new_err(format!("read_object stream failed for {obj}: {e}"))
                        })? {
                            let copy_len = chunk.len().min(cap.saturating_sub(written));
                            if copy_len > 0 {
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        chunk.as_ptr(),
                                        dst.0.add(written),
                                        copy_len,
                                    );
                                }
                                written += copy_len;
                            }
                        }
                        Ok::<_, PyErr>((Some(target), written, Vec::new()))
                    } else {
                        let mut buf = Vec::new();
                        while let Some(chunk) = reader.next().await.transpose().map_err(|e| {
                            PyIOError::new_err(format!("read_object stream failed for {obj}: {e}"))
                        })? {
                            buf.extend_from_slice(&chunk);
                        }
                        Ok::<_, PyErr>((None, buf.len(), buf))
                    }
                }
            },
        );

        let results = try_join_all(tasks).await?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for (target_opt, written, fallback_vec) in results {
                if let Some(target) = target_opt {
                    let cap_ptr =
                        unsafe { pyo3::ffi::PyBytes_AsString(target.as_ptr()) as *const u8 };
                    let expected_len =
                        unsafe { pyo3::ffi::PyBytes_Size(target.as_ptr()) as usize };
                    if written == expected_len {
                        list.append(target)?;
                    } else {
                        let slice = unsafe { std::slice::from_raw_parts(cap_ptr, written) };
                        list.append(PyBytes::new(py, slice))?;
                    }
                } else {
                    list.append(PyBytes::new(py, &fallback_vec))?;
                }
            }
            Ok(list.unbind())
        })
    })
}

/// Write an object using the stable Rust `Storage::write_object` HTTP/JSON upload endpoint.
#[pyfunction]
#[pyo3(signature = (bucket_name, object_name, data, endpoint=None))]
fn json_write_object<'py>(
    py: Python<'py>,
    bucket_name: String,
    object_name: String,
    data: &[u8],
    endpoint: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let payload = Bytes::copy_from_slice(data);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let clients = get_clients(endpoint, None).await?;
        let obj = clients
            .storage
            .write_object(bucket_resource(&bucket_name), &object_name, payload)
            .send_unbuffered()
            .await
            .map_err(|e| {
                PyIOError::new_err(format!(
                    "write_object failed for {bucket_name}/{object_name}: {e}"
                ))
            })?;
        Python::attach(|py| Ok(object_to_pydict(py, &obj)?.unbind()))
    })
}

#[pymodule]
fn _rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads()).enable_all();
    // Ignore error if runtime was already initialized in this process
    let _ = pyo3_async_runtimes::tokio::init(builder);

    m.add_class::<RustMultiRangeDownloader>()?;
    m.add_class::<RustAppendableObjectWriter>()?;
    m.add_function(wrap_pyfunction!(list_objects_async, m)?)?;
    m.add_function(wrap_pyfunction!(json_read_ranges_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(json_write_object, m)?)?;
    Ok(())
}

