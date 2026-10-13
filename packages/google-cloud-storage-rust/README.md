# google-cloud-storage-rust

Experimental Google Cloud Storage client package for Python that supports side-by-side execution with either the pure-Python SDK backend (`backend="python"`) or a compiled Rust PyO3 backend (`backend="rust"`) built on the official [`google-cloud-storage`](https://docs.rs/google-cloud-storage) Rust SDK.

## Supported Operations

### 1. HTTP/JSON Operations (Standard Regional & Multi-Regional Buckets)
1. **ReadObject / Concurrent Range Reads (`AsyncGrpcClient.json_read_ranges` / `_rust_core.json_read_ranges_bytes`)**: Concurrent byte-range and full-object downloads over the HTTP/JSON API (`Storage::read_object` via `reqwest` on Tokio in Rust vs `google.cloud.storage.Client` / `urllib3` in Python).
2. **WriteObject (`AsyncGrpcClient.json_write_object` / `_rust_core.json_write_object`)**: Object uploads over the HTTP/JSON API (`Storage::write_object` in Rust vs `Blob.upload_from_string` in Python).

### 2. Bidirectional gRPC & Control Operations (Zonal / Rapid & Standard Buckets)
3. **BidiRead (`AsyncMultiRangeDownloader`)**: Multiplexed concurrent byte-range downloads over `BidiReadObject` (`Storage::open_object` / `ObjectDescriptor` in Rust vs `_StreamMultiplexer` + `grpc.aio` in Python), supporting both `download_ranges` (`BytesIO`) and zero-copy `download_ranges_bytes` (`list[bytes]`).
4. **BidiWrite (`AsyncAppendableObjectWriter`)**: Streaming appendable object uploads, flushes, and finalization over `BidiWriteObject` (`Storage::open_appendable_object` / `AppendableObjectWriter` in Rust vs `_AsyncWriteObjectStream` in Python).
5. **ListObjects (`AsyncGrpcClient.list_objects`)**: Paginated object metadata listing (`StorageControl::list_objects` in Rust vs GAPIC `StorageAsyncClient.list_objects` / `Client.list_blobs` in Python).

## Quick Start

```python
import asyncio
from io import BytesIO
from google.cloud.storage_rust import (
    AsyncAppendableObjectWriter,
    AsyncGrpcClient,
    AsyncMultiRangeDownloader,
)

async def main() -> None:
    # Toggle between backend="rust" and backend="python"
    client = AsyncGrpcClient(backend="rust", attempt_direct_path=False)

    # 1. HTTP/JSON Read & Write (Standard Regional Buckets)
    await client.json_write_object("my-regional-bucket", "sample.bin", b"hello-rust" * 1000)
    ranges = await client.json_read_ranges("my-regional-bucket", "sample.bin", [(0, 100), (100, 200)])

    # 2. BidiRead (Zonal / Rapid Buckets via AsyncMultiRangeDownloader)
    mrd = await AsyncMultiRangeDownloader.create_mrd(
        client, bucket_name="my-rapid-bucket", object_name="checkpoint.pt"
    )
    buf1, buf2 = BytesIO(), BytesIO()
    await mrd.download_ranges([(0, 65536, buf1), (65536, 65536, buf2)])
    raw_chunks = await mrd.download_ranges_bytes([(0, 65536), (65536, 65536)])
    await mrd.close()

    # 3. BidiWrite (Zonal / Rapid Buckets via AsyncAppendableObjectWriter)
    writer = AsyncAppendableObjectWriter(
        client, bucket_name="my-rapid-bucket", object_name="output.log"
    )
    await writer.open()
    await writer.append(b"chunk-1")
    await writer.flush()
    await writer.finalize()

    # 4. ListObjects
    objects, prefixes = await client.list_objects("my-rapid-bucket", prefix="logs/")

asyncio.run(main())
```

## Build, Test, and Benchmark

```bash
cd /home/princer_google_com/dev/google-cloud-python/packages/google-cloud-storage-rust

# Build and install in the repo virtualenv
VIRTUAL_ENV=/home/princer_google_com/dev/google-cloud-python/.venv \
  /home/princer_google_com/dev/google-cloud-python/.venv/bin/maturin develop --release

# Run unit tests
/home/princer_google_com/dev/google-cloud-python/.venv/bin/pytest tests/test_storage_rust.py -v

# Run live HTTP/JSON benchmarks (princer-flat)
GCS_JSON_BUCKET=princer-flat \
  /home/princer_google_com/dev/google-cloud-python/.venv/bin/python benchmarks/bench_json_workload.py

# Run live CloudPath gRPC Rapid benchmarks (princer-rapid-bucket)
GCS_RAPID_BUCKET=princer-rapid-bucket \
  /home/princer_google_com/dev/google-cloud-python/.venv/bin/python benchmarks/bench_rapid_workload.py
```
