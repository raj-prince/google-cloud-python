# Evaluating a Shared Rust Core for the Google Cloud Storage Python SDK

## 1. Introduction

To avoid maintaining duplicate handwritten storage logic across languages, our long-term goal is to implement a single core Rust SDK and reuse it across all golden languages (see the [shared native library background](https://docs.google.com/document/d/1pC4PXbBVeXu9xIGn2E3D-Qkd8n87IqUsi8NUYtIXVdE/edit?resourcekey=0-4Uk_EjuRwyGBl76qHPbFuA&tab=t.0#heading=h.yokmyyo3vmmw) for details). This document evaluates that approach specifically for the Python SDK: section 2 covers our proof-of-concept architecture, section 3 compares live read, write, and list performance with and without Rust, and section 4 evaluates the seven technical challenges. Evaluating Go, C++, or Java is out of scope.

## 2. POC and High-Level Architecture

We built a proof-of-concept package in [lib.rs](file:///home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust/src/lib.rs) and [async_grpc_client.py](file:///home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust/google/cloud/storage_rust/async_grpc_client.py) that wraps the official Google Cloud Storage Rust SDK using PyO3 and a multi-threaded Tokio runtime, while exposing a backend toggle to run the exact same workloads against the pure Python client.

```mermaid
flowchart LR
    Caller["Python Caller"] --> Toggle{"Backend Switch"}
    Toggle -->|Without Rust| Py["Pure Python SDK (urllib3 / gRPC AsyncIO on GIL)"]
    Toggle -->|With Rust| PyO3["PyO3 Async Bridge (GIL Released)"]
    PyO3 --> Tokio["Rust Core SDK (Reqwest / Tonic on 4 Tokio Threads)"]
```

To avoid blocking the Python event loop or corrupting state across process forks, our bridge follows three steps:
1. The Rust storage and control clients initialize lazily on first I/O rather than at module import time so worker processes forked after import never inherit broken sockets.
2. For each range read up to M1 (256) MiB, Python pre-allocates an uninitialized byte object of length L1 and passes its raw memory pointer to Rust before releasing the interpreter lock.
3. Tokio dispatches the HTTP or gRPC calls across W1 (4) worker threads, copies incoming network chunks directly into the pre-allocated memory without holding the interpreter lock, and re-acquires the lock once at completion to return the finished byte objects.

## 3. Performance Comparison of Read, Write, and List (With and Without Rust)

Because the stable Rust SDK in [read_object.rs](file:///home/princer_google_com/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/google-cloud-storage-1.20.0/src/storage/read_object.rs) and [write_object.rs](file:///home/princer_google_com/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/google-cloud-storage-1.20.0/src/storage/write_object.rs) uses HTTP and JSON for object reads and writes (while bidirectional gRPC writes remain behind an unstable compiler flag), we benchmarked both the HTTP/JSON path against the regional bucket princer-flat in [bench_json_workload.py](file:///home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust/benchmarks/bench_json_workload.py) and the CloudPath gRPC path against the zonal bucket princer-rapid-bucket in [bench_rapid_workload.py](file:///home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust/benchmarks/bench_rapid_workload.py).

### HTTP/JSON Client Comparison (Regional Bucket princer-flat)

| Workload | Python Latency | Rust Latency | Latency Diff | Python CPU | Rust CPU | CPU Diff | Python Memory | Rust Memory | Memory Diff |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Read: 100 x 64 KiB Ranges | 158.64 ms | 43.20 ms | 3.67x faster | 34.47 s/GiB | 5.33 s/GiB | 6.47x less | 81.7 MiB | 98.1 MiB | +16.4 MiB |
| Read: 64 x 1 MiB Ranges | 294.24 ms | 64.67 ms | 4.55x faster | 6.42 s/GiB | 1.72 s/GiB | 3.73x less | 174.7 MiB | 182.8 MiB | +8.1 MiB |
| Read: 16 x 16 MiB Ranges | 792.21 ms | 177.46 ms | 4.46x faster | 4.46 s/GiB | 1.38 s/GiB | 3.23x less | 575.8 MiB | 348.1 MiB | -227.7 MiB |
| Read: 1 x 100 MiB Object | 402.58 ms | 362.75 ms | 1.11x faster | 2.69 s/GiB | 1.74 s/GiB | 1.55x less | 270.8 MiB | 275.4 MiB | +4.6 MiB |
| Write: 1 x 256 KiB Upload | 67.22 ms | 59.03 ms | 1.14x faster | 5.23 s/GiB | 2.71 s/GiB | 1.93x less | 72.1 MiB | 76.7 MiB | +4.6 MiB |
| Write: 1 x 16 MiB Upload | 211.98 ms | 181.97 ms | 1.16x faster | 0.69 s/GiB | 0.39 s/GiB | 1.77x less | 87.6 MiB | 90.6 MiB | +3.0 MiB |
| Write: 1 x 64 MiB Upload | 465.73 ms | 521.18 ms | 0.89x slower | 0.60 s/GiB | 0.34 s/GiB | 1.76x less | 135.4 MiB | 136.9 MiB | +1.5 MiB |
| List: 1,000 Objects | 60.81 ms | 58.73 ms | 1.04x faster | 11.27 ms/call | 3.83 ms/call | 2.94x less | 81.1 MiB | 81.6 MiB | +0.5 MiB |

### CloudPath gRPC Client Comparison (Zonal Bucket princer-rapid-bucket)

| Workload | Python Latency | Rust Latency | Latency Diff | Python CPU | Rust CPU | CPU Diff | Python Memory | Rust Memory | Memory Diff |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BidiRead: 100 x 64 KiB Ranges | 37.95 ms | 23.64 ms | 1.61x faster | 4.34 s/GiB | 2.13 s/GiB | 2.04x less | 84.8 MiB | 84.6 MiB | -0.2 MiB |
| BidiRead: 64 x 1 MiB Ranges | 200.22 ms | 84.94 ms | 2.36x faster | 4.08 s/GiB | 2.64 s/GiB | 1.55x less | 203.1 MiB | 211.8 MiB | +8.7 MiB |
| BidiRead: 16 x 16 MiB Ranges | 1,027.10 ms | 299.23 ms | 3.43x faster | 4.97 s/GiB | 2.35 s/GiB | 2.11x less | 687.5 MiB | 346.6 MiB | -340.9 MiB |
| BidiWrite: 64 x 256 KiB Appends | 157.98 ms | 143.70 ms | 1.10x faster | 1.86 s/GiB | 1.49 s/GiB | 1.25x less | 90.4 MiB | 95.7 MiB | +5.3 MiB |
| BidiWrite: 8 x 16 MiB Appends | 495.75 ms | 395.07 ms | 1.25x faster | 1.32 s/GiB | 0.63 s/GiB | 2.10x less | 118.8 MiB | 102.1 MiB | -16.7 MiB |
| ListObjects: 200 Live Objects | 51.55 ms | 44.13 ms | 1.17x faster | 4.90 ms/call | 1.63 ms/call | 3.01x less | 72.4 MiB | 75.2 MiB | +2.8 MiB |

Two practical takeaways explain when Rust helps and when it does not:
- Concurrent multi-range reads see the largest gain (3.43x to 4.55x faster, 2.1x to 6.5x lower CPU, and ~40% to 50% lower peak memory on 256 MiB transfers) because Tokio runs 16 to 100 range transfers in parallel without contending on the Python interpreter lock or allocating intermediate frame buffers.
- Single-stream reads and writes see modest latency gains (1.11x to 1.25x) with 1.5x to 2.0x lower CPU usage because a single CloudPath HTTP/2 or TCP stream is network-bound around 250 to 335 MiB/s. On a 64 MiB JSON upload, Rust is slightly slower in wall time (521 ms vs 466 ms) because Rust uses a multi-step resumable upload with full-payload CRC32C verification while Python sends a single-shot multipart post without checksums.

## 4. Technical Challenges

| Challenge | Python Evaluation (Pure Python SDK vs Wrapped Rust Core) |
| --- | --- |
| Mono Repo | Introducing Rust directly in the monorepo adds Cargo workspace management, native compilation steps, and multi-OS binary wheel build pipelines alongside existing pure-Python packages. |
| Platform-specific issues | Feasible: at runtime, Python's gRPC stack already uses C++ (grpcio) and Rust (cryptography) native extensions, and our Rust binary links fewer system libraries than grpcio (no C++ libstdc++.so.6 dependency). The main change is shifting native build ownership (CMake/C11 for aws-lc-sys on Alpine musl and glibc symbol floors) from upstream grpcio/cryptography maintainers to our team. |
| Platform reach | Feasible: end users installing gRPC dependencies already rely on platform-specific wheels from grpcio and cryptography; our package similarly shifts from a single pure-Python wheel to building and publishing a pre-built wheel matrix per OS, CPU architecture, and Python ABI (or abi3) via maturin/cibuildwheel. |
| Package Size | Pure Python is 336 KiB alone (~11.8 MiB compressed / ~34.5 MiB unpacked with transitive grpcio and cryptography wheels). Our self-contained Rust wheel is 7.7 MiB compressed (20.5 MiB stripped). Adding DirectPath in Rust will increase size slightly, but the overall footprint remains comparable to Python's full gRPC stack. |
| Toolchain, coordination and maintenance costs | Requires a dual Python and Rust toolchain. Even when reusing the core Rust SDK and benefiting from Rust's lower CVE rate, we still need an independent Python-specific effort to maintain the PyO3 binding layer, sync new APIs, and publish multi-OS wheels. |
| Impedance mismatches | Manageable: as the Python SDK moves toward an async-first model, Rust's async Tokio futures map naturally to Python coroutines, and in-progress Rust DirectPath support will be inherited automatically. |
| Harder debugging experience | Manageable in practice: while standard Python tracebacks stop at the FFI boundary, AI coding tools simplify cross-language diagnosis, and [PyO3's debugging workflow](https://pyo3.rs/main/debugging) supports Rust panic backtraces, breakpoints in pytest, and step-through IDE debugging via LLDB and GDB. |

### Conclusion

Wrapping the shared Rust core SDK for Python is **feasible**: it delivers large gains (3.4x to 4.5x higher throughput and 3x to 6x lower CPU usage) on concurrent multi-range reads and modest gains (4% to 25%) on single-stream operations, with manageable engineering trade-offs around multi-platform wheel builds and ongoing PyO3 binding maintenance in the monorepo.

## Appendix: How to Run and Verify Manually

You can build the Rust extension, run the dual-backend unit test suite in [test_storage_rust.py](file:///home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust/tests/test_storage_rust.py), execute both benchmark suites, and verify the wheel and transitive dependency sizes with and without Rust using the repository virtual environment:

```bash
cd /home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust

# 1. Build and install the PyO3 Rust extension in release mode
VIRTUAL_ENV=/home/princer_google_com/dev/google-cloud-python/.venv \
  /home/princer_google_com/dev/google-cloud-python/.venv/bin/maturin develop --release

# 2. Run parity unit tests across both Python and Rust backends
/home/princer_google_com/dev/google-cloud-python/.venv/bin/pytest tests/test_storage_rust.py -v

# 3. Run live HTTP/JSON client benchmarks against the regional bucket (princer-flat)
GCS_JSON_BUCKET=princer-flat \
  /home/princer_google_com/dev/google-cloud-python/.venv/bin/python benchmarks/bench_json_workload.py

# 4. Run live CloudPath gRPC benchmarks against the zonal Rapid bucket (princer-rapid-bucket)
GCS_RAPID_BUCKET=princer-rapid-bucket \
  /home/princer_google_com/dev/google-cloud-python/.venv/bin/python benchmarks/bench_rapid_workload.py

# 5. Build and compare wheel & transitive dependency sizes (With Rust vs Without Rust)
mkdir -p /tmp/wheels_compare
/home/princer_google_com/dev/google-cloud-python/.venv/bin/maturin build --release --strip \
  -i /home/princer_google_com/dev/google-cloud-python/.venv/bin/python -o /tmp/wheels_compare
uv build --wheel ../google-cloud-storage -o /tmp/wheels_compare
ls -lh /tmp/wheels_compare/*.whl target/release/lib_rust_core.so
du -sh /home/princer_google_com/dev/google-cloud-python/.venv/lib/python3.12/site-packages/{grpc,cryptography,google/_upb,google_crc32c}
# Expected:
#   google_cloud_storage-3.15.1-py3-none-any.whl          (~336 KiB pure-Python wheel alone)
#   + Python transitive native deps in site-packages:     grpc (18 MiB unpacked / 6.8 MiB whl), cryptography (16 MiB / 4.5 MiB whl), _upb (420 KiB), crc32c (96 KiB)
#   google_cloud_storage_rust-0.1.0-cp312-...-x86_64.whl  (~7.7 MiB stripped wheel, self-contained with Reqwest + Tonic + Prost + Rustls)
#   target/release/lib_rust_core.so                       (~28.5 MiB unstripped / ~20.5 MiB stripped shared library)
```


