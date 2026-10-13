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

"""Dual-backend AsyncAppendableObjectWriter (BidiWriteObject) for Python vs Rust comparison."""

from __future__ import annotations

from io import BufferedReader
from typing import Any, List, Optional, Tuple, Union

from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
    AsyncAppendableObjectWriter as NativePythonAsyncAppendableObjectWriter,
)
from google.cloud.storage_rust.async_grpc_client import (
    AsyncGrpcClient,
    ObjectMetadata,
)


class AsyncAppendableObjectWriter:
    """Appends data to a GCS Appendable Object over ``BidiWriteObject``.

    Mirrors :class:`google.cloud.storage.asyncio.async_appendable_object_writer.AsyncAppendableObjectWriter`
    and dispatches to either the Rust PyO3 ``AppendableObjectWriter`` backend or
    the pure-Python ``_AsyncWriteObjectStream`` backend depending on ``client.backend``.
    """

    def __init__(
        self,
        client: AsyncGrpcClient,
        bucket_name: str,
        object_name: str,
        generation: Optional[int] = None,
        write_handle: Any = None,
        writer_options: Optional[dict] = None,
        storage_class: Optional[str] = None,
    ) -> None:
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.generation = generation
        self.write_handle = write_handle
        self.writer_options = writer_options or {}
        self.storage_class = storage_class
        self.offset: Optional[int] = None
        self.persisted_size: Optional[int] = None
        self._rust_writer: Any = None
        self._python_writer: Optional[NativePythonAsyncAppendableObjectWriter] = None

        if client.backend == "rust":
            from google.cloud.storage_rust import _rust_core

            self._rust_writer = _rust_core.RustAppendableObjectWriter(
                bucket_name, object_name, generation
            )
        else:
            self._python_writer = NativePythonAsyncAppendableObjectWriter(
                client,  # type: ignore[arg-type]
                bucket_name,
                object_name,
                generation=generation,
                write_handle=write_handle,
                writer_options=writer_options,
                storage_class=storage_class,
            )

    async def open(
        self,
        retry_policy: Any = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        """Open the underlying ``BidiWriteObject`` stream."""
        if self._rust_writer is not None:
            if self.client.simulated_payload is not None:
                self._rust_writer.open_simulated(self.generation or 1)
                self.generation = self._rust_writer.generation
                self.persisted_size = self._rust_writer.persisted_size
                self.offset = self._rust_writer.offset
            else:
                gen_out, persisted_out = await self._rust_writer.open(
                    endpoint=self.client.endpoint,
                    subchannel_count=self.client.subchannel_count,
                )
                self._rust_writer._set_open_state(gen_out, persisted_out)
                self.generation = gen_out
                self.persisted_size = persisted_out
                self.offset = persisted_out
            return

        assert self._python_writer is not None
        await self._python_writer.open(retry_policy=retry_policy, metadata=metadata)
        self.generation = self._python_writer.generation
        self.persisted_size = self._python_writer.persisted_size
        self.offset = self._python_writer.offset
        self.write_handle = self._python_writer.write_handle

    async def append(
        self,
        data: bytes,
        retry_policy: Any = None,
        metadata: Optional[List[Tuple[str, str]]] = None,
        enable_checksum: bool = True,
    ) -> None:
        """Append ``data`` to the open appendable object."""
        if self._rust_writer is not None:
            if not data:
                return
            await self._rust_writer.append(data)
            self.offset = self._rust_writer.offset
            return

        assert self._python_writer is not None
        await self._python_writer.append(
            data,
            retry_policy=retry_policy,
            metadata=metadata,
            enable_checksum=enable_checksum,
        )
        self.offset = self._python_writer.offset
        self.persisted_size = self._python_writer.persisted_size

    async def flush(self) -> int:
        """Flush buffered data and return the server-acknowledged ``persisted_size``."""
        if self._rust_writer is not None:
            persisted = await self._rust_writer.flush()
            self.persisted_size = int(persisted)
            self.offset = self.persisted_size
            return self.persisted_size

        assert self._python_writer is not None
        persisted = await self._python_writer.flush()
        self.persisted_size = persisted
        self.offset = self._python_writer.offset
        return persisted

    async def simple_flush(self) -> None:
        """Flush without waiting for a state lookup."""
        if self._rust_writer is not None:
            await self._rust_writer.flush()
            self.persisted_size = self._rust_writer.persisted_size
            return

        assert self._python_writer is not None
        await self._python_writer.simple_flush()

    async def state_lookup(self) -> int:
        """Query and return the current server ``persisted_size``."""
        if self._rust_writer is not None:
            return await self.flush()

        assert self._python_writer is not None
        persisted = await self._python_writer.state_lookup()
        self.persisted_size = persisted
        return persisted

    async def finalize(
        self,
        full_object_checksum: Optional[int] = None,
        retry_policy: Any = None,
    ) -> Union[ObjectMetadata, Any]:
        """Finalize the appendable object on the server and close the stream."""
        if self._rust_writer is not None:
            raw_dict = await self._rust_writer.finalize()
            meta = ObjectMetadata.from_dict(raw_dict)
            self.persisted_size = meta.size
            self.offset = None
            return meta

        assert self._python_writer is not None
        res = await self._python_writer.finalize(
            full_object_checksum=full_object_checksum,
            retry_policy=retry_policy,
        )
        self.persisted_size = self._python_writer.persisted_size
        self.offset = None
        return res

    async def close(
        self,
        finalize_on_close: bool = False,
        full_object_checksum: Optional[int] = None,
        retry_policy: Any = None,
    ) -> Union[int, ObjectMetadata, Any]:
        """Close the ``BidiWriteObject`` stream (optionally finalizing the object)."""
        if self._rust_writer is not None:
            if finalize_on_close:
                return await self.finalize(
                    full_object_checksum=full_object_checksum,
                    retry_policy=retry_policy,
                )
            persisted = await self._rust_writer.close(finalize_on_close=False)
            self.persisted_size = int(persisted)
            return self.persisted_size

        assert self._python_writer is not None
        res = await self._python_writer.close(
            finalize_on_close=finalize_on_close,
            full_object_checksum=full_object_checksum,
            retry_policy=retry_policy,
        )
        self.persisted_size = self._python_writer.persisted_size
        return res

    async def append_from_file(
        self,
        file_obj: BufferedReader,
        block_size: int = _DEFAULT_FLUSH_INTERVAL_BYTES,
    ) -> None:
        """Append bytes from a binary file handle in ``block_size`` chunks."""
        while block := file_obj.read(block_size):
            await self.append(block)

    @property
    def is_stream_open(self) -> bool:
        """Return True if the underlying ``BidiWriteObject`` stream is open."""
        if self._rust_writer is not None:
            return bool(self._rust_writer.is_stream_open)
        assert self._python_writer is not None
        return self._python_writer.is_stream_open

