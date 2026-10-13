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

"""Dual-backend AsyncMultiRangeDownloader (BidiReadObject) for Python vs Rust comparison."""

from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Any, List, Optional, Tuple

from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader as NativePythonAsyncMultiRangeDownloader,
)
from google.cloud.storage_rust.async_grpc_client import AsyncGrpcClient


class AsyncMultiRangeDownloader:
    """Downloads multiple byte ranges concurrently over ``BidiReadObject``.

    Mirrors :class:`google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader`
    and dispatches to either the Rust PyO3 ``ObjectDescriptor`` backend or the
    pure-Python ``_StreamMultiplexer`` backend depending on ``client.backend``.
    """

    @classmethod
    async def create_mrd(
        cls,
        client: AsyncGrpcClient,
        bucket_name: str,
        object_name: str,
        generation: Optional[int] = None,
        read_handle: Any = None,
        retry_policy: Any = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        **kwargs: Any,
    ) -> AsyncMultiRangeDownloader:
        """Initialize and open a multi-range downloader."""
        mrd = cls(
            client,
            bucket_name,
            object_name,
            generation=generation,
            read_handle=read_handle,
            **kwargs,
        )
        await mrd.open(retry_policy=retry_policy, metadata=metadata)
        return mrd

    def __init__(
        self,
        client: AsyncGrpcClient,
        bucket_name: str,
        object_name: str,
        generation: Optional[int] = None,
        read_handle: Any = None,
        **kwargs: Any,
    ) -> None:
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.generation = generation
        self.read_handle = read_handle
        self.persisted_size: Optional[int] = None
        self._rust_mrd: Any = None
        self._python_mrd: Optional[NativePythonAsyncMultiRangeDownloader] = None

        if client.backend == "rust":
            from google.cloud.storage_rust import _rust_core

            self._rust_mrd = _rust_core.RustMultiRangeDownloader(
                bucket_name, object_name, generation
            )
        else:
            self._python_mrd = NativePythonAsyncMultiRangeDownloader(
                client,  # type: ignore[arg-type]
                bucket_name,
                object_name,
                generation=generation,
                read_handle=read_handle,
                **kwargs,
            )

    async def __aenter__(self) -> AsyncMultiRangeDownloader:
        if not self.is_stream_open:
            await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.is_stream_open:
            await self.close()

    async def open(
        self,
        retry_policy: Any = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        """Open the underlying ``BidiReadObject`` stream."""
        if self._rust_mrd is not None:
            if self.client.simulated_payload is not None:
                self._rust_mrd.open_simulated(self.client.simulated_payload)
                self.generation = self._rust_mrd.generation
                self.persisted_size = self._rust_mrd.persisted_size
            else:
                gen_out, size_out = await self._rust_mrd.open(
                    endpoint=self.client.endpoint,
                    subchannel_count=self.client.subchannel_count,
                )
                self._rust_mrd._set_open_metadata(gen_out, size_out)
                self.generation = gen_out
                self.persisted_size = size_out
            return

        assert self._python_mrd is not None
        await self._python_mrd.open(retry_policy=retry_policy, metadata=metadata)
        self.generation = self._python_mrd.generation
        self.persisted_size = self._python_mrd.persisted_size
        self.read_handle = self._python_mrd.read_handle

    async def download_ranges(
        self,
        read_ranges: List[Tuple[int, int, BytesIO]],
        lock: Optional[asyncio.Lock] = None,
        retry_policy: Any = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        enable_checksum: bool = True,
    ) -> None:
        """Download multiple byte ranges into user-supplied writable buffers.

        Matches the exact signature of
        :meth:`google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.download_ranges`.
        """
        if self._rust_mrd is not None:
            raw_ranges = [(int(offset), int(length)) for offset, length, _ in read_ranges]
            byte_chunks: List[bytes] = await self._rust_mrd.download_ranges_bytes(
                raw_ranges
            )
            for (_, _, user_buffer), chunk in zip(read_ranges, byte_chunks):
                user_buffer.write(chunk)
            return

        assert self._python_mrd is not None
        await self._python_mrd.download_ranges(
            read_ranges,
            lock=lock,
            retry_policy=retry_policy,
            metadata=metadata,
            enable_checksum=enable_checksum,
        )

    async def download_ranges_bytes(
        self,
        ranges: List[Tuple[int, int]],
        enable_checksum: bool = True,
    ) -> List[bytes]:
        """Download multiple ``(offset, length)`` byte ranges and return ``List[bytes]`` directly.

        On the Rust backend, this avoids creating intermediate ``BytesIO`` objects
        in Python and fills pre-allocated ``PyBytes`` buffers directly from Tokio worker threads.
        """
        if self._rust_mrd is not None:
            return await self._rust_mrd.download_ranges_bytes(ranges)

        buffers = [BytesIO() for _ in ranges]
        tuple_ranges = [
            (offset, length, buf) for (offset, length), buf in zip(ranges, buffers)
        ]
        await self.download_ranges(tuple_ranges, enable_checksum=enable_checksum)
        return [buf.getvalue() for buf in buffers]

    async def close(self) -> None:
        """Close the underlying ``BidiReadObject`` stream."""
        if self._rust_mrd is not None:
            await self._rust_mrd.close()
            return
        assert self._python_mrd is not None
        await self._python_mrd.close()

    @property
    def is_stream_open(self) -> bool:
        """Return True if the underlying ``BidiReadObject`` stream is open."""
        if self._rust_mrd is not None:
            return bool(self._rust_mrd.is_stream_open)
        assert self._python_mrd is not None
        return self._python_mrd.is_stream_open

