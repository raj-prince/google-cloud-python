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

"""Tests comparing Native Python and Wrapped Rust (PyO3) backends for BidiRead, BidiWrite, and ListObjects."""

from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Literal

import pytest

from google.cloud.storage_rust import (
    AsyncAppendableObjectWriter,
    AsyncGrpcClient,
    AsyncMultiRangeDownloader,
)

BackendMode = Literal["rust", "python"]


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("rust", id="rust-backend"),
        pytest.param("python", id="python-backend"),
    ],
)
def test_bidi_read_multi_range_downloader_matches_across_backends(
    backend: BackendMode,
) -> None:
    # Arrange
    payload = b"0123456789abcdef" * (64 * 1024)  # 1 MiB
    client = AsyncGrpcClient(backend=backend, simulated_payload=payload)
    buf1 = BytesIO()
    buf2 = BytesIO()
    buf3 = BytesIO()

    async def _run() -> None:
        mrd = await AsyncMultiRangeDownloader.create_mrd(
            client,
            bucket_name="rapid-test-bucket",
            object_name="checkpoint-001.bin",
        )
        assert mrd.is_stream_open is True
        assert mrd.persisted_size == len(payload)
        await mrd.download_ranges(
            [
                (0, 1024, buf1),
                (4096, 8192, buf2),
                (100_000, 65536, buf3),
            ]
        )
        await mrd.close()
        assert mrd.is_stream_open is False

    # Act
    asyncio.run(_run())

    # Assert
    assert buf1.getvalue() == payload[0:1024]
    assert buf2.getvalue() == payload[4096 : 4096 + 8192]
    assert buf3.getvalue() == payload[100_000 : 100_000 + 65536]


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("rust", id="rust-backend"),
        pytest.param("python", id="python-backend"),
    ],
)
def test_bidi_write_appendable_object_writer_matches_across_backends(
    backend: BackendMode,
) -> None:
    # Arrange
    chunk_a = b"A" * (512 * 1024)
    chunk_b = b"B" * (512 * 1024)
    client = AsyncGrpcClient(backend=backend, simulated_payload=b"sim")

    async def _run() -> tuple[int, int]:
        writer = AsyncAppendableObjectWriter(
            client,
            bucket_name="rapid-test-bucket",
            object_name="wal-segment-01.log",
        )
        await writer.open()
        await writer.append(chunk_a)
        await writer.append(chunk_b)
        flushed_size = await writer.flush()
        final_meta = await writer.finalize()
        final_size = getattr(final_meta, "size", 0)
        return flushed_size, int(final_size)

    # Act
    flushed_size, final_size = asyncio.run(_run())

    # Assert
    expected_total = len(chunk_a) + len(chunk_b)
    assert flushed_size == expected_total
    assert final_size == expected_total


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("rust", id="rust-backend"),
        pytest.param("python", id="python-backend"),
    ],
)
def test_list_objects_pagination_matches_across_backends(
    backend: BackendMode,
) -> None:
    # Arrange
    client = AsyncGrpcClient(backend=backend, simulated_payload=b"sim")

    async def _run():
        return await client.list_objects(
            "rapid-test-bucket",
            prefix="checkpoints/step_100/",
            simulated_count=250,
        )

    # Act
    objects, prefixes = asyncio.run(_run())

    # Assert
    assert len(objects) == 250
    assert prefixes == []
    assert objects[0].name == "checkpoints/step_100/object_000000.bin"
    assert objects[249].name == "checkpoints/step_100/object_000249.bin"
    assert objects[0].storage_class == "RAPID"
    assert objects[0].size == 16 * 1024 * 1024

