# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Benchmark comparing Native Python JSON Client vs Wrapped Rust SDK JSON/HTTP Client.

Workloads tested against a standard regional bucket (default: ``princer-flat``):
1. ReadObject / Range Reads (HTTP GET with Range header):
   - 100 x 64 KiB concurrent ranges (High-IOPS metadata/index lookup)
   - 64 x 1 MiB concurrent ranges (Medium tensor/parquet column chunks)
   - 16 x 16 MiB concurrent ranges (256 MiB total download)
   - 1 x 100 MiB single-stream full object download
2. WriteObject (HTTP Multipart / Unbuffered Upload):
   - 1 x 256 KiB object upload
   - 1 x 16 MiB object upload
   - 1 x 64 MiB object upload
3. ListObjects:
   - 1,000 objects listed from ``princer-flat``
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
import os
from pathlib import Path
import statistics
import time
from typing import Any, Dict, List, Literal

from google.cloud import storage
from requests.adapters import HTTPAdapter

from google.cloud.storage_rust import _rust_core

BackendMode = Literal["python_json", "rust_json"]


def _create_python_json_client(pool_maxsize: int = 128) -> storage.Client:
    """Create a Python ``google.cloud.storage.Client`` with a sized urllib3 connection pool."""
    client = storage.Client()
    adapter = HTTPAdapter(pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
    client._http.mount("https://", adapter)
    client._http.mount("http://", adapter)
    return client


async def bench_json_read(
    backend: BackendMode,
    py_client: storage.Client,
    bucket_name: str,
    object_name: str,
    obj_size: int,
    num_ranges: int,
    range_size: int,
    iterations: int = 5,
    use_bytesio: bool = False,
) -> Dict[str, Any]:
    """Benchmark HTTP/JSON range reads: Python ``storage.Client`` vs Rust ``Storage::read_object``."""
    max_offset = max(0, obj_size - range_size)
    step = max(1, max_offset // max(1, num_ranges))
    ranges_spec = [
        ((i * step) % max(1, max_offset), range_size) for i in range(num_ranges)
    ]

    bucket = py_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    max_workers = max(1, num_ranges)

    def _py_download_one(off_len: tuple[int, int]) -> bytes:
        off, length = off_len
        data = blob.download_as_bytes(
            start=off,
            end=off + length - 1,
            raw_download=True,
            checksum=None,
        )
        if use_bytesio:
            buf = BytesIO()
            buf.write(data)
        return data

    pool = ThreadPoolExecutor(max_workers=max_workers) if backend == "python_json" else None
    try:
        async def _run_once() -> None:
            if backend == "python_json":
                assert pool is not None
                loop = asyncio.get_running_loop()
                await asyncio.gather(
                    *(
                        loop.run_in_executor(pool, _py_download_one, spec)
                        for spec in ranges_spec
                    )
                )
            else:
                chunks = await _rust_core.json_read_ranges_bytes(
                    bucket_name, object_name, ranges_spec
                )
                if use_bytesio:
                    for chunk in chunks:
                        buf = BytesIO()
                        buf.write(chunk)

        # Warmup (warms both thread pool and all num_ranges HTTPS connections)
        await _run_once()

        wall_times: List[float] = []
        cpu_times: List[float] = []
        total_bytes_per_iter = num_ranges * range_size

        for _ in range(iterations):
            t0_wall = time.perf_counter()
            t0_cpu = time.process_time()
            await _run_once()
            wall_times.append(time.perf_counter() - t0_wall)
            cpu_times.append(time.process_time() - t0_cpu)
    finally:
        if pool is not None:
            pool.shutdown(wait=True)


    median_wall = statistics.median(wall_times)
    p99_wall = sorted(wall_times)[int(len(wall_times) * 0.99) - 1]
    mean_cpu = statistics.mean(cpu_times)
    gib_per_iter = total_bytes_per_iter / (1024**3)
    throughput_mib_s = (total_bytes_per_iter / (1024**2)) / median_wall
    cpu_s_per_gib = mean_cpu / gib_per_iter if gib_per_iter > 0 else 0.0

    return {
        "operation": (
            "ReadObject (BytesIO)" if use_bytesio else "ReadObject (ZeroCopy PyBytes)"
        ),
        "backend": backend,
        "workload": f"{num_ranges} x {range_size // 1024} KiB",
        "median_latency_ms": round(median_wall * 1000.0, 2),
        "p99_latency_ms": round(p99_wall * 1000.0, 2),
        "throughput_mib_s": round(throughput_mib_s, 1),
        "cpu_s_per_gib": round(cpu_s_per_gib, 3),
    }


async def bench_json_write(
    backend: BackendMode,
    py_client: storage.Client,
    bucket_name: str,
    payload_size: int,
    iterations: int = 5,
) -> Dict[str, Any]:
    """Benchmark HTTP/JSON object writes: Python ``blob.upload_from_string`` vs Rust ``Storage::write_object``."""
    payload = b"J" * payload_size
    bucket = py_client.bucket(bucket_name)

    wall_times: List[float] = []
    cpu_times: List[float] = []

    for idx in range(iterations + 1):
        object_name = f"bench/tmp_json_write_{backend}_{payload_size}_{idx}.bin"
        t0_wall = time.perf_counter()
        t0_cpu = time.process_time()
        if backend == "python_json":
            blob = bucket.blob(object_name)
            await asyncio.to_thread(
                blob.upload_from_string,
                payload,
                content_type="application/octet-stream",
            )
        else:
            await _rust_core.json_write_object(bucket_name, object_name, payload)
        dt_wall = time.perf_counter() - t0_wall
        dt_cpu = time.process_time() - t0_cpu
        if idx > 0:
            wall_times.append(dt_wall)
            cpu_times.append(dt_cpu)

    median_wall = statistics.median(wall_times)
    p99_wall = sorted(wall_times)[int(len(wall_times) * 0.99) - 1]
    mean_cpu = statistics.mean(cpu_times)
    gib_per_iter = payload_size / (1024**3)
    throughput_mib_s = (payload_size / (1024**2)) / median_wall
    cpu_s_per_gib = mean_cpu / gib_per_iter if gib_per_iter > 0 else 0.0

    return {
        "operation": "WriteObject (HTTP Upload)",
        "backend": backend,
        "workload": f"1 x {payload_size // 1024} KiB",
        "median_latency_ms": round(median_wall * 1000.0, 2),
        "p99_latency_ms": round(p99_wall * 1000.0, 2),
        "throughput_mib_s": round(throughput_mib_s, 1),
        "cpu_s_per_gib": round(cpu_s_per_gib, 3),
    }


async def bench_json_list(
    backend: BackendMode,
    py_client: storage.Client,
    bucket_name: str,
    prefix: str,
    object_count: int = 1000,
    iterations: int = 5,
) -> Dict[str, Any]:
    """Benchmark ListObjects: Python JSON ``client.list_blobs`` vs Rust ``StorageControl::list_objects``."""

    def _py_list() -> int:
        blobs = list(
            py_client.list_blobs(
                bucket_name,
                prefix=prefix,
                max_results=object_count,
                page_size=min(1000, object_count),
            )
        )
        return len(blobs)

    async def _run_once() -> int:
        if backend == "python_json":
            return await asyncio.to_thread(_py_list)
        raw = await _rust_core.list_objects_async(
            bucket_name,
            prefix=prefix,
            max_results=object_count,
        )
        return len(raw["objects"])

    actual_count = await _run_once()
    wall_times: List[float] = []
    cpu_times: List[float] = []

    for _ in range(iterations):
        t0_wall = time.perf_counter()
        t0_cpu = time.process_time()
        cnt = await _run_once()
        assert cnt == actual_count
        wall_times.append(time.perf_counter() - t0_wall)
        cpu_times.append(time.process_time() - t0_cpu)

    median_wall = statistics.median(wall_times)
    p99_wall = sorted(wall_times)[int(len(wall_times) * 0.99) - 1]
    mean_cpu = statistics.mean(cpu_times)
    objs_per_sec = actual_count / median_wall

    return {
        "operation": "ListObjects (Paginator)",
        "backend": backend,
        "workload": f"{actual_count:,} objects",
        "median_latency_ms": round(median_wall * 1000.0, 2),
        "p99_latency_ms": round(p99_wall * 1000.0, 2),
        "objects_per_sec": round(objs_per_sec, 0),
        "cpu_ms_per_call": round(mean_cpu * 1000.0, 2),
    }


async def run_json_benchmarks() -> Dict[str, Any]:
    bucket_name = os.environ.get("GCS_JSON_BUCKET", "princer-flat")
    py_client = _create_python_json_client(pool_maxsize=128)

    results: List[Dict[str, Any]] = []

    # 1. ReadObject Benchmarks against bench/bench_20GB.bin
    obj_name = "bench/bench_20GB.bin"
    obj_sz = 20 * 1024 * 1024 * 1024
    for num_ranges, range_size in [
        (100, 64 * 1024),
        (64, 1024 * 1024),
        (16, 16 * 1024 * 1024),
        (1, 100 * 1024 * 1024),
    ]:
        for backend in ("python_json", "rust_json"):
            res = await bench_json_read(
                backend=backend,  # type: ignore[arg-type]
                py_client=py_client,
                bucket_name=bucket_name,
                object_name=obj_name,
                obj_size=obj_sz,
                num_ranges=num_ranges,
                range_size=range_size,
                use_bytesio=False,
            )
            results.append(res)


    # 2. WriteObject Benchmarks
    for payload_size in [
        256 * 1024,        # 256 KiB
        16 * 1024 * 1024,  # 16 MiB
        64 * 1024 * 1024,  # 64 MiB
    ]:
        for backend in ("python_json", "rust_json"):
            res = await bench_json_write(
                backend=backend,  # type: ignore[arg-type]
                py_client=py_client,
                bucket_name=bucket_name,
                payload_size=payload_size,
            )
            results.append(res)

    # 3. ListObjects Benchmarks (1,000 objects from princer-flat)
    for obj_count in (1000,):
        for backend in ("python_json", "rust_json"):
            res = await bench_json_list(
                backend=backend,  # type: ignore[arg-type]
                py_client=py_client,
                bucket_name=bucket_name,
                prefix="benchmark-static/list_flat_1procs_1threads_131072files_0depth_1folders_ls_regional/folder_0/",
                object_count=obj_count,
            )
            results.append(res)

    summary = {
        "mode": f"live_json_bucket ({bucket_name})",
        "results": results,
    }
    out_path = Path(__file__).parent / "json_live_benchmark_results.json"
    out_path.write_text(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    summary = asyncio.run(run_json_benchmarks())
    print(f"\n=== JSON/HTTP Workload Benchmark Summary ({summary['mode']}) ===")
    print(
        f"{'Operation':<30} | {'Workload':<16} | {'Backend':<12} | {'Median (ms)':>11} | {'p99 (ms)':>9} | {'Throughput / Rate':>20} | {'CPU Cost':>16}"
    )
    print("-" * 128)
    for row in summary["results"]:
        if "throughput_mib_s" in row:
            rate_str = f"{row['throughput_mib_s']:,.1f} MiB/s"
            cpu_str = f"{row['cpu_s_per_gib']:.3f} CPU-s/GiB"
        else:
            rate_str = f"{row['objects_per_sec']:,.0f} obj/s"
            cpu_str = f"{row['cpu_ms_per_call']:.2f} ms/call"
        print(
            f"{row['operation']:<30} | {row['workload']:<16} | {row['backend']:<12} | {row['median_latency_ms']:>11.2f} | {row['p99_latency_ms']:>9.2f} | {rate_str:>20} | {cpu_str:>16}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

