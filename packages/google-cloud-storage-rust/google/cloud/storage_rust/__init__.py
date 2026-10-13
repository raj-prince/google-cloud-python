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

"""Google Cloud Storage client with selectable Rust (PyO3) and Python backends.

Provides drop-in compatible classes for Rapid / Zonal and Standard GCS workloads:
- :class:`AsyncGrpcClient` (with ``list_objects`` / ``list_blobs``)
- :class:`AsyncMultiRangeDownloader` (``BidiReadObject``)
- :class:`AsyncAppendableObjectWriter` (``BidiWriteObject``)
"""

from google.cloud.storage_rust.async_appendable_object_writer import (
    AsyncAppendableObjectWriter,
)
from google.cloud.storage_rust.async_grpc_client import (
    AsyncGrpcClient,
    ObjectMetadata,
)
from google.cloud.storage_rust.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

__version__ = "0.1.0"

__all__ = [
    "AsyncAppendableObjectWriter",
    "AsyncGrpcClient",
    "AsyncMultiRangeDownloader",
    "ObjectMetadata",
    "__version__",
]

