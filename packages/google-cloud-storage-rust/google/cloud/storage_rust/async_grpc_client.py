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

"""Dual-backend AsyncGrpcClient supporting both Native Python and Rust (PyO3) execution."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

import google_crc32c
import grpc
from google.cloud import _storage_v2 as storage_v2
from google.cloud.storage.asyncio.async_grpc_client import (
    AsyncGrpcClient as NativePythonAsyncGrpcClient,
)

BackendType = Literal["rust", "python"]


@dataclass(frozen=True)
class ObjectMetadata:
    """Lightweight object metadata returned by ``list_objects`` and ``finalize``."""

    name: str
    bucket: str
    generation: int
    size: int
    storage_class: str = "STANDARD"
    metageneration: int = 1
    etag: str = ""
    crc32c: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ObjectMetadata:
        """Construct an :class:`ObjectMetadata` from a Rust PyO3 dictionary."""
        return cls(
            name=data.get("name", ""),
            bucket=data.get("bucket", ""),
            generation=int(data.get("generation", 0)),
            size=int(data.get("size", 0)),
            storage_class=data.get("storage_class", "STANDARD"),
            metageneration=int(data.get("metageneration", 1)),
            etag=data.get("etag", ""),
            crc32c=data.get("crc32c"),
        )

    @classmethod
    def from_proto(cls, proto_obj: Any) -> ObjectMetadata:
        """Construct an :class:`ObjectMetadata` from a ``_storage_v2.Object`` protobuf."""
        crc = None
        if getattr(proto_obj, "checksums", None) and proto_obj.checksums.crc32c:
            crc = proto_obj.checksums.crc32c
        return cls(
            name=proto_obj.name,
            bucket=proto_obj.bucket,
            generation=proto_obj.generation,
            size=proto_obj.size,
            storage_class=proto_obj.storage_class,
            metageneration=proto_obj.metageneration,
            etag=proto_obj.etag,
            crc32c=crc,
        )


class _SimulatedBidiReadCall:
    """Simulated gRPC-asyncio BidiReadObject call exercising the full Python AsyncBidiRpc + GAPIC + proto-plus path."""

    def __init__(
        self,
        request_generator: Any,
        payload: bytes,
        chunk_size: int = 2 * 1024 * 1024,
    ) -> None:
        self._payload = payload
        self._chunk_size = max(4096, chunk_size)
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._opened = False
        self._cancelled = False
        self._callbacks: List[Any] = []
        self._pump_task = asyncio.create_task(self._consume_requests(request_generator))

    def add_done_callback(self, callback: Any) -> None:
        self._callbacks.append(callback)

    def done(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True
        self._pump_task.cancel()
        self._queue.put_nowait(grpc.aio.EOF)

    async def _consume_requests(self, request_generator: Any) -> None:
        try:
            async for request in request_generator:
                if request is None or self._cancelled:
                    break
                await self._handle_request(request)
            await self._queue.put(grpc.aio.EOF)
        except asyncio.CancelledError:
            pass

    async def _handle_request(
        self, request: storage_v2.BidiReadObjectRequest
    ) -> None:
        if not self._opened:
            self._opened = True
            full_crc = google_crc32c.value(self._payload)
            meta = storage_v2.Object(
                name=request.read_object_spec.object,
                bucket=request.read_object_spec.bucket,
                generation=request.read_object_spec.generation or 1,
                size=len(self._payload),
                checksums=storage_v2.ObjectChecksums(crc32c=full_crc),
            )
            handle = storage_v2.BidiReadHandle(handle=b"sim-read-handle")
            await self._queue.put(
                storage_v2.BidiReadObjectResponse(
                    metadata=meta,
                    read_handle=handle,
                )
            )
            if not request.read_ranges:
                return

        for rr in request.read_ranges:
            start = min(rr.read_offset, len(self._payload))
            end = (
                len(self._payload)
                if rr.read_length == 0
                else min(start + rr.read_length, len(self._payload))
            )
            view = self._payload[start:end]
            offset = start
            total = len(view)
            pos = 0
            while pos < total or total == 0:
                chunk = view[pos : pos + self._chunk_size]
                pos += len(chunk)
                is_end = pos >= total
                crc = google_crc32c.value(chunk)
                data_range = storage_v2.ObjectRangeData(
                    checksummed_data=storage_v2.ChecksummedData(
                        content=chunk,
                        crc32c=crc,
                    ),
                    read_range=storage_v2.ReadRange(
                        read_offset=offset,
                        read_length=len(chunk),
                        read_id=rr.read_id,
                    ),
                    range_end=is_end,
                )
                offset += len(chunk)
                await self._queue.put(
                    storage_v2.BidiReadObjectResponse(
                        object_data_ranges=[data_range],
                    )
                )
                if total == 0:
                    break

    async def read(self) -> Any:
        return await self._queue.get()


class _SimulatedBidiWriteCall:
    """Simulated gRPC-asyncio BidiWriteObject call exercising the full Python AsyncBidiRpc + GAPIC + proto-plus path."""

    def __init__(
        self,
        request_generator: Any,
        bucket: str = "projects/_/buckets/rapid-test-bucket",
        object_name: str = "sim-object",
        generation: int = 1,
    ) -> None:
        self._bucket = bucket
        self._object_name = object_name
        self._generation = generation
        self._buffer = bytearray()
        self._persisted_size = 0
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._opened = False
        self._cancelled = False
        self._callbacks: List[Any] = []
        self._pump_task = asyncio.create_task(self._consume_requests(request_generator))

    def add_done_callback(self, callback: Any) -> None:
        self._callbacks.append(callback)

    def done(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True
        self._pump_task.cancel()
        self._queue.put_nowait(grpc.aio.EOF)

    async def _consume_requests(self, request_generator: Any) -> None:
        try:
            async for request in request_generator:
                if request is None or self._cancelled:
                    break
                await self._handle_request(request)
            await self._queue.put(grpc.aio.EOF)
        except asyncio.CancelledError:
            pass

    async def _handle_request(
        self, request: storage_v2.BidiWriteObjectRequest
    ) -> None:
        if not self._opened:
            self._opened = True
            if request.write_object_spec and request.write_object_spec.resource:
                self._bucket = request.write_object_spec.resource.bucket or self._bucket
                self._object_name = (
                    request.write_object_spec.resource.name or self._object_name
                )
            await self._queue.put(
                storage_v2.BidiWriteObjectResponse(
                    persisted_size=0,
                    write_handle=storage_v2.BidiWriteHandle(handle=b"sim-write-handle"),
                )
            )
            return

        if request.checksummed_data and request.checksummed_data.content:
            content = request.checksummed_data.content
            if request.checksummed_data.crc32c:
                actual = google_crc32c.value(content)
                if actual != request.checksummed_data.crc32c:
                    raise ValueError("CRC32C mismatch in simulated BidiWriteObject")
            self._buffer.extend(content)

        if request.finish_write:
            self._persisted_size = len(self._buffer)
            obj = storage_v2.Object(
                name=self._object_name,
                bucket=self._bucket,
                generation=self._generation,
                size=self._persisted_size,
                storage_class="RAPID",
            )
            await self._queue.put(storage_v2.BidiWriteObjectResponse(resource=obj))
        elif request.flush or request.state_lookup:
            if request.flush:
                self._persisted_size = len(self._buffer)
            if request.state_lookup:
                await self._queue.put(
                    storage_v2.BidiWriteObjectResponse(
                        persisted_size=self._persisted_size,
                        write_handle=storage_v2.BidiWriteHandle(
                            handle=b"sim-write-handle"
                        ),
                    )
                )

    async def read(self) -> Any:
        return await self._queue.get()


class _SimulatedTransport:
    """Simulated GAPIC transport providing ``_wrapped_methods`` for ``bidi_read_object`` and ``bidi_write_object``."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.bidi_read_object = "bidi_read_object_rpc"
        self.bidi_write_object = "bidi_write_object_rpc"
        self._wrapped_methods = {
            self.bidi_read_object: self._start_bidi_read,
            self.bidi_write_object: self._start_bidi_write,
        }

    async def _start_bidi_read(
        self, request_generator: Any, metadata: Any = None
    ) -> _SimulatedBidiReadCall:
        return _SimulatedBidiReadCall(request_generator, self._payload)

    async def _start_bidi_write(
        self, request_generator: Any, metadata: Any = None
    ) -> _SimulatedBidiWriteCall:
        return _SimulatedBidiWriteCall(request_generator)


class _SimulatedInnerClient:
    def __init__(self, payload: bytes) -> None:
        self._transport = _SimulatedTransport(payload)


class _SimulatedGrpcAsyncClient:
    """Simulated GAPIC StorageAsyncClient that constructs real ``_storage_v2`` Protobuf messages."""

    def __init__(self, payload: bytes = b"") -> None:
        self.payload = payload
        self._client = _SimulatedInnerClient(payload)

    async def list_objects(
        self,
        request: storage_v2.ListObjectsRequest,
        metadata: Sequence[Tuple[str, str]] = (),
        timeout: Optional[float] = None,
        retry: Any = None,
        simulated_count: int = 1000,
    ) -> List[storage_v2.ListObjectsResponse]:
        # Construct real proto-plus Object messages in pages of 1000 to mirror GAPIC behavior.
        pages: List[storage_v2.ListObjectsResponse] = []
        page_size = 1000
        for page_start in range(0, simulated_count, page_size):
            batch_end = min(page_start + page_size, simulated_count)
            objs = [
                storage_v2.Object(
                    name=f"{request.prefix}object_{i:06d}.bin",
                    bucket=request.parent,
                    generation=1_700_000_000_000_000 + i,
                    metageneration=1,
                    size=16 * 1024 * 1024,
                    storage_class="RAPID",
                    etag=f"etag-{i:06d}",
                )
                for i in range(page_start, batch_end)
            ]
            pages.append(storage_v2.ListObjectsResponse(objects=objs))
        return pages


class AsyncGrpcClient:
    """Asynchronous client for Google Cloud Storage with selectable ``rust`` or ``python`` backend.

    Args:
        credentials: Optional OAuth2 Credentials for the Python backend.
        client_info: Optional ClientInfo for user-agent metadata.
        client_options: Optional ClientOptions (e.g., ``api_endpoint``).
        attempt_direct_path: Whether to attempt DirectPath for gRPC connections.
        backend: Execution backend, either ``"rust"`` (PyO3 + Tokio) or ``"python"`` (pure Python GAPIC).
        subchannel_count: Optional gRPC subchannel count for the Rust backend.
        simulated_payload: Optional byte payload for local Rapid workload benchmarking without network I/O.
    """

    def __init__(
        self,
        credentials: Any = None,
        client_info: Any = None,
        client_options: Any = None,
        *,
        attempt_direct_path: bool = True,
        backend: BackendType = "rust",
        subchannel_count: Optional[int] = None,
        simulated_payload: Optional[bytes] = None,
    ) -> None:
        if backend not in ("rust", "python"):
            raise ValueError(f"Unsupported backend {backend!r}; expected 'rust' or 'python'.")

        self.backend: BackendType = backend
        self.attempt_direct_path = attempt_direct_path
        self.subchannel_count = subchannel_count
        self.client_options = client_options
        self.endpoint: Optional[str] = (
            getattr(client_options, "api_endpoint", None) if client_options else None
        )
        self.simulated_payload = simulated_payload
        self._native_client: Optional[NativePythonAsyncGrpcClient] = None
        self._simulated_grpc_client: Optional[_SimulatedGrpcAsyncClient] = None

        if simulated_payload is not None:
            self._simulated_grpc_client = _SimulatedGrpcAsyncClient(simulated_payload)
        elif backend == "python":
            self._native_client = NativePythonAsyncGrpcClient(
                credentials=credentials,
                client_info=client_info,
                client_options=client_options,
                attempt_direct_path=attempt_direct_path,
            )

    @property
    def grpc_client(self) -> Any:
        """Return the underlying GAPIC ``StorageAsyncClient`` (or simulated equivalent)."""
        if self._simulated_grpc_client is not None:
            return self._simulated_grpc_client
        if self._native_client is None:
            self._native_client = NativePythonAsyncGrpcClient(
                client_options=self.client_options,
                attempt_direct_path=self.attempt_direct_path,
            )
        return self._native_client.grpc_client

    async def list_objects(
        self,
        bucket_name: str,
        *,
        prefix: str = "",
        delimiter: Optional[str] = None,
        versions: bool = False,
        include_folders_as_prefixes: bool = False,
        max_results: Optional[int] = None,
        simulated_count: Optional[int] = None,
    ) -> Tuple[List[ObjectMetadata], List[str]]:
        """List objects in ``bucket_name`` using either the Rust or Python gRPC backend.

        Args:
            bucket_name: Name of the GCS bucket.
            prefix: Filter results to objects whose names begin with this prefix.
            delimiter: Optional delimiter (such as ``"/"``) for hierarchy folding.
            versions: If True, list all generations of objects.
            include_folders_as_prefixes: Include Managed Folders / HNS folders in prefixes.
            max_results: Optional maximum number of objects to return.
            simulated_count: Optional object count for local serialization/pagination benchmarks.

        Returns:
            A tuple of ``(objects, prefixes)``.
        """
        if self.backend == "rust":
            from google.cloud.storage_rust import _rust_core

            raw = await _rust_core.list_objects_async(
                bucket_name,
                prefix=prefix,
                delimiter=delimiter,
                versions=versions,
                include_folders_as_prefixes=include_folders_as_prefixes,
                max_results=max_results,
                endpoint=self.endpoint,
                subchannel_count=self.subchannel_count,
                simulated_count=simulated_count,
            )
            objects = [ObjectMetadata.from_dict(item) for item in raw["objects"]]
            prefixes = list(raw["prefixes"])
            return objects, prefixes

        # Native Python backend via _storage_v2.ListObjectsRequest
        parent = (
            bucket_name
            if bucket_name.startswith("projects/")
            else f"projects/_/buckets/{bucket_name}"
        )
        request = storage_v2.ListObjectsRequest(
            parent=parent,
            prefix=prefix,
            delimiter=delimiter or "",
            versions=versions,
            include_folders_as_prefixes=include_folders_as_prefixes,
            page_size=min(max_results, 1000) if max_results else 1000,
        )
        objects: List[ObjectMetadata] = []
        prefixes: List[str] = []

        if self._simulated_grpc_client is not None:
            count = simulated_count if simulated_count is not None else (max_results or 1000)
            pages = await self._simulated_grpc_client.list_objects(
                request, simulated_count=count
            )
            for page in pages:
                for obj in page.objects:
                    objects.append(ObjectMetadata.from_proto(obj))
                prefixes.extend(page.prefixes)
            return objects, prefixes

        pager = await self.grpc_client.list_objects(request=request)
        async for page in pager.pages:
            for obj in page.objects:
                objects.append(ObjectMetadata.from_proto(obj))
                if max_results and len(objects) >= max_results:
                    return objects, prefixes
            prefixes.extend(page.prefixes)
        return objects, prefixes

    async def json_read_ranges(
        self,
        bucket_name: str,
        object_name: str,
        ranges: Sequence[Tuple[int, int]],
    ) -> List[bytes]:
        """Read one or more ``(offset, length)`` byte ranges over the HTTP/JSON API.

        Uses ``Storage::read_object`` (``reqwest``) when ``backend="rust"`` or
        ``google.cloud.storage.Client`` (``urllib3``) when ``backend="python"``.
        """
        if self.backend == "rust":
            from google.cloud.storage_rust import _rust_core

            return list(
                await _rust_core.json_read_ranges_bytes(
                    bucket_name,
                    object_name,
                    list(ranges),
                    endpoint=self.endpoint,
                )
            )

        from concurrent.futures import ThreadPoolExecutor
        from google.cloud import storage

        json_client = storage.Client()
        blob = json_client.bucket(bucket_name).blob(object_name)

        def _read_one(spec: Tuple[int, int]) -> bytes:
            offset, length = spec
            end = (offset + length - 1) if length > 0 else None
            return blob.download_as_bytes(
                start=offset,
                end=end,
                raw_download=True,
                checksum=None,
            )

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=min(32, max(1, len(ranges)))) as pool:
            return list(
                await asyncio.gather(
                    *(loop.run_in_executor(pool, _read_one, r) for r in ranges)
                )
            )

    async def json_write_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
    ) -> ObjectMetadata:
        """Upload an object over the HTTP/JSON API.

        Uses ``Storage::write_object`` (``reqwest``) when ``backend="rust"`` or
        ``blob.upload_from_string`` (``urllib3``) when ``backend="python"``.
        """
        if self.backend == "rust":
            from google.cloud.storage_rust import _rust_core

            raw = await _rust_core.json_write_object(
                bucket_name,
                object_name,
                data,
                endpoint=self.endpoint,
            )
            return ObjectMetadata.from_dict(raw)

        from google.cloud import storage

        json_client = storage.Client()
        blob = json_client.bucket(bucket_name).blob(object_name)
        await asyncio.to_thread(
            blob.upload_from_string,
            data,
            content_type="application/octet-stream",
        )
        return ObjectMetadata(
            name=blob.name or object_name,
            bucket=bucket_name,
            generation=int(blob.generation or 0),
            size=int(blob.size or len(data)),
            storage_class=blob.storage_class or "STANDARD",
            metageneration=int(blob.metageneration or 1),
            etag=blob.etag or "",
        )

