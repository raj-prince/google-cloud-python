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

"""Benchmark comparing Native Python vs Wrapped Rust (PyO3) across Rapid Storage workloads.

Workloads tested:
1. BidiRead (AsyncMultiRangeDownloader):
   - 100 x 64 KiB random ranges (High-IOPS metadata/index lookup)
   - 64 x 1 MiB ranges (Medium tensor/parquet column chunks)
   - 16 x 16 MiB ranges (Large AI checkpoint shards)
2. BidiWrite (AsyncAppendableObjectWriter):
   - 64 x 256 KiB appends + flush + finalize (WAL / streaming log workload)
   - 8 x 16 MiB appends + flush + finalize (Large checkpoint write workload)
3. ListObjects (AsyncGrpcClient.list_objects):
   - Live bucket listing or simulated pagination benchmark

Set ``GCS_RAPID_BUCKET=<zonal-bucket-name>`` to run against a live Zonal (Rapid)
bucket; when unset, runs the deterministic in-memory Rapid protocol harness.
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Literal

from google.cloud.storage_rust import (
    AsyncAppendableObjectWriter,
    AsyncGrpcClient,
    AsyncMultiRangeDownloader,
)

BackendMode = Literal["python", "rust"]


async def bench_bidi_read(
    backend: BackendMode,
    payload: bytes,
    num_ranges: int,
    range_size: int,
    iterations: int = 5,
    use_zero_copy_bytes: bool = False,
    subchannel_count: int = 1,
    live_bucket: str | None = None,
    live_object: str | None = None,
) -> Dict[str, Any]:
    """Benchmark BidiRead (AsyncMultiRangeDownloader) over CloudPath across Native Python vs Wrapped Rust."""
    client = AsyncGrpcClient(
        backend=backend,
        attempt_direct_path=False,
        simulated_payload=None if live_bucket else payload,
        subchannel_count=subchannel_count if live_bucket else None,
    )
    bucket_name = live_bucket or "rapid-bench-bucket"
    object_name = live_object or "bench/bench_1GB.bin"

    mrd = await AsyncMultiRangeDownloader.create_mrd(
        client,
        bucket_name=bucket_name,
        object_name=object_name,
    )
    obj_size = mrd.persisted_size or len(payload)
    max_offset = max(0, obj_size - range_size)
    step = max(1, max_offset // max(1, num_ranges))
    ranges_spec = [
        ((i * step) % max(1, max_offset), range_size) for i in range(num_ranges)
    ]

    # Warmup
    if use_zero_copy_bytes:
        await mrd.download_ranges_bytes(ranges_spec)
    else:
        warmup_bufs = [(off, sz, BytesIO()) for off, sz in ranges_spec]
        await mrd.download_ranges(warmup_bufs)

    wall_times: List[float] = []
    cpu_times: List[float] = []
    total_bytes_per_iter = num_ranges * range_size

    for _ in range(iterations):
        t0_wall = time.perf_counter()
        t0_cpu = time.process_time()
        if use_zero_copy_bytes:
            await mrd.download_ranges_bytes(ranges_spec)
        else:
            req_bufs = [(off, sz, BytesIO()) for off, sz in ranges_spec]
            await mrd.download_ranges(req_bufs)
        wall_times.append(time.perf_counter() - t0_wall)
        cpu_times.append(time.process_time() - t0_cpu)

    await mrd.close()

    median_wall = statistics.median(wall_times)
    p99_wall = sorted(wall_times)[int(len(wall_times) * 0.99) - 1]
    mean_cpu = statistics.mean(cpu_times)
    gib_per_iter = total_bytes_per_iter / (1024**3)
    throughput_mib_s = (total_bytes_per_iter / (1024**2)) / median_wall
    cpu_s_per_gib = mean_cpu / gib_per_iter if gib_per_iter > 0 else 0.0

    backend_label = (
        f"{backend} ({subchannel_count}ch)"
        if live_bucket and backend == "rust" and subchannel_count > 1
        else backend
    )

    return {
        "operation": (
            "BidiRead (ZeroCopy PyBytes)"
            if use_zero_copy_bytes
            else "BidiRead (BytesIO)"
        ),
        "backend": backend_label,
        "transport": "cloudpath" if live_bucket else "simulated",
        "workload": f"{num_ranges} x {range_size // 1024} KiB",
        "median_latency_ms": round(median_wall * 1000.0, 2),
        "p99_latency_ms": round(p99_wall * 1000.0, 2),
        "throughput_mib_s": round(throughput_mib_s, 1),
        "cpu_s_per_gib": round(cpu_s_per_gib, 3),
    }


async def bench_bidi_write(
    backend: BackendMode,
    num_appends: int,
    append_size: int,
    iterations: int = 5,
    live_bucket: str | None = None,
) -> Dict[str, Any]:
    """Benchmark BidiWrite (AsyncAppendableObjectWriter) over CloudPath across Native Python vs Wrapped Rust."""
    chunk = b"W" * append_size
    client = AsyncGrpcClient(
        backend=backend,
        attempt_direct_path=False,
        simulated_payload=None if live_bucket else b"sim",
        subchannel_count=1 if live_bucket else None,
    )
    bucket_name = live_bucket or "rapid-bench-bucket"

    wall_times: List[float] = []
    cpu_times: List[float] = []
    total_bytes_per_iter = num_appends * append_size

    for idx in range(iterations + 1):
        writer = AsyncAppendableObjectWriter(
            client,
            bucket_name=bucket_name,
            object_name=f"bench/tmp_rapid_write_bench_{backend}_{append_size}_{idx}.bin",
        )
        t0_wall = time.perf_counter()
        t0_cpu = time.process_time()
        await writer.open()
        for _ in range(num_appends):
            await writer.append(chunk)
        await writer.flush()
        await writer.finalize()
        dt_wall = time.perf_counter() - t0_wall
        dt_cpu = time.process_time() - t0_cpu
        if idx > 0:  # skip warmup iteration 0
            wall_times.append(dt_wall)
            cpu_times.append(dt_cpu)

    median_wall = statistics.median(wall_times)
    p99_wall = sorted(wall_times)[int(len(wall_times) * 0.99) - 1]
    mean_cpu = statistics.mean(cpu_times)
    gib_per_iter = total_bytes_per_iter / (1024**3)
    throughput_mib_s = (total_bytes_per_iter / (1024**2)) / median_wall
    cpu_s_per_gib = mean_cpu / gib_per_iter if gib_per_iter > 0 else 0.0

    return {
        "operation": "BidiWrite (Append+Flush+Finalize)",
        "backend": backend,
        "transport": "cloudpath" if live_bucket else "simulated",
        "workload": f"{num_appends} x {append_size // 1024} KiB",
        "median_latency_ms": round(median_wall * 1000.0, 2),
        "p99_latency_ms": round(p99_wall * 1000.0, 2),
        "throughput_mib_s": round(throughput_mib_s, 1),
        "cpu_s_per_gib": round(cpu_s_per_gib, 3),
    }


async def ensure_live_list_objects(bucket_name: str, target_count: int = 200) -> int:
    """Ensure at least ``target_count`` objects exist under ``bench/list_pool/`` for live ListObjects benchmarking."""
    client = AsyncGrpcClient(backend="rust", attempt_direct_path=False)
    existing, _ = await client.list_objects(
        bucket_name, prefix="bench/list_pool/", max_results=target_count
    )
    missing = target_count - len(existing)
    if missing > 0:
        sem = asyncio.Semaphore(32)

        async def _create_one(idx: int) -> None:
            async with sem:
                w = AsyncAppendableObjectWriter(
                    client,
                    bucket_name=bucket_name,
                    object_name=f"bench/list_pool/obj_{idx:05d}.bin",
                )
                await w.open()
                await w.append(b"x" * 64)
                await w.finalize()

        await asyncio.gather(*(_create_one(i) for i in range(len(existing), target_count)))
    return target_count


async def bench_list_objects(
    backend: BackendMode,
    object_count: int,
    iterations: int = 8,
    live_bucket: str | None = None,
    prefix: str = "rapid/train/step_",
) -> Dict[str, Any]:
    """Benchmark ListObjects pagination and deserialization over CloudPath across Native Python vs Wrapped Rust."""
    client = AsyncGrpcClient(
        backend=backend,
        attempt_direct_path=False,
        simulated_payload=None if live_bucket else b"sim",
    )
    bucket_name = live_bucket or "rapid-bench-bucket"

    # Warmup
    objs_warm, _ = await client.list_objects(
        bucket_name,
        prefix=prefix,
        max_results=object_count,
        simulated_count=None if live_bucket else object_count,
    )
    actual_count = len(objs_warm) if live_bucket else object_count

    wall_times: List[float] = []
    cpu_times: List[float] = []

    for _ in range(iterations):
        t0_wall = time.perf_counter()
        t0_cpu = time.process_time()
        objs, _ = await client.list_objects(
            bucket_name,
            prefix=prefix,
            max_results=object_count,
            simulated_count=None if live_bucket else object_count,
        )
        assert len(objs) == actual_count
        wall_times.append(time.perf_counter() - t0_wall)
        cpu_times.append(time.process_time() - t0_cpu)

    median_wall = statistics.median(wall_times)
    p99_wall = sorted(wall_times)[int(len(wall_times) * 0.99) - 1]
    mean_cpu = statistics.mean(cpu_times)
    objs_per_sec = actual_count / median_wall

    return {
        "operation": "ListObjects (gRPC Paginator)",
        "backend": backend,
        "transport": "cloudpath" if live_bucket else "simulated",
        "workload": f"{actual_count:,} objects",
        "median_latency_ms": round(median_wall * 1000.0, 2),
        "p99_latency_ms": round(p99_wall * 1000.0, 2),
        "objects_per_sec": round(objs_per_sec, 0),
        "cpu_ms_per_call": round(mean_cpu * 1000.0, 2),
    }


async def run_all_benchmarks() -> Dict[str, Any]:
    live_bucket = os.environ.get("GCS_RAPID_BUCKET")
    payload_256mb = b"" if live_bucket else b"R" * (256 * 1024 * 1024)

    results: List[Dict[str, Any]] = []

    # 1. BidiRead Benchmarks (CloudPath for both Python and Rust)
    for num_ranges, range_size in [
        (100, 64 * 1024),        # 100 x 64 KiB
        (64, 1024 * 1024),       # 64 x 1 MiB
        (16, 16 * 1024 * 1024),  # 16 x 16 MiB (256 MiB)
    ]:
        for backend in ("python", "rust"):
            res = await bench_bidi_read(
                backend=backend,  # type: ignore[arg-type]
                payload=payload_256mb,
                num_ranges=num_ranges,
                range_size=range_size,
                subchannel_count=1,
                live_bucket=live_bucket,
                live_object="bench/bench_1GB.bin" if live_bucket else None,
            )
            results.append(res)
        if live_bucket:
            res_8ch = await bench_bidi_read(
                backend="rust",
                payload=payload_256mb,
                num_ranges=num_ranges,
                range_size=range_size,
                subchannel_count=8,
                live_bucket=live_bucket,
                live_object="bench/bench_1GB.bin" if live_bucket else None,
            )
            results.append(res_8ch)
        if range_size == 16 * 1024 * 1024:
            res_zc = await bench_bidi_read(
                backend="rust",
                payload=payload_256mb,
                num_ranges=num_ranges,
                range_size=range_size,
                use_zero_copy_bytes=True,
                subchannel_count=8 if live_bucket else 1,
                live_bucket=live_bucket,
                live_object="bench/bench_1GB.bin" if live_bucket else None,
            )
            results.append(res_zc)

    # 2. BidiWrite Benchmarks (CloudPath for both Python and Rust)
    for num_appends, append_size in [
        (64, 256 * 1024),       # 64 x 256 KiB (16 MiB)
        (8, 16 * 1024 * 1024),  # 8 x 16 MiB (128 MiB)
    ]:
        for backend in ("python", "rust"):
            res = await bench_bidi_write(
                backend=backend,  # type: ignore[arg-type]
                num_appends=num_appends,
                append_size=append_size,
                live_bucket=live_bucket,
            )
            results.append(res)

    # 3. ListObjects Benchmarks (CloudPath for both Python and Rust)
    if live_bucket:
        await ensure_live_list_objects(live_bucket, target_count=200)
        for backend in ("python", "rust"):
            res = await bench_list_objects(
                backend=backend,  # type: ignore[arg-type]
                object_count=200,
                live_bucket=live_bucket,
                prefix="bench/list_pool/",
            )
            results.append(res)
    else:
        for obj_count in (1000, 10000):
            for backend in ("python", "rust"):
                res = await bench_list_objects(
                    backend=backend,  # type: ignore[arg-type]
                    object_count=obj_count,
                    live_bucket=None,
                )
                results.append(res)

    summary = {
        "mode": (
            f"live_rapid_bucket_cloudpath ({live_bucket})"
            if live_bucket
            else "simulated_rapid_protocol"
        ),
        "results": results,
    }
    out_file = (
        "rapid_live_benchmark_results.json"
        if live_bucket
        else "rapid_benchmark_results.json"
    )
    out_path = Path(__file__).parent / out_file
    out_path.write_text(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    summary = asyncio.run(run_all_benchmarks())
    print(f"\n=== Rapid Workload Benchmark Summary ({summary['mode']}) ===")
    print(
        f"{'Operation':<34} | {'Workload':<16} | {'Backend':<8} | {'Median (ms)':>11} | {'p99 (ms)':>9} | {'Throughput / Rate':>20} | {'CPU Cost':>16}"
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
            f"{row['operation']:<34} | {row['workload']:<16} | {row['backend']:<8} | "
            f"{row['median_latency_ms']:>11.2f} | {row['p99_latency_ms']:>9.2f} | "
            f"{rate_str:>20} | {cpu_str:>16}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
