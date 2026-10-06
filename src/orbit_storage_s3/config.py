# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Validated bucket, endpoint, and bounded transfer settings for S3."""

from __future__ import annotations

import math
from dataclasses import dataclass
from urllib.parse import urlsplit

from orbit_storage import StorageConfigurationError

_MIN_PART_SIZE = 5 * 1024 * 1024
_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class S3Config:
    """S3 bucket settings; authentication is delegated to aiobotocore's credential chain.

    ``endpoint_url`` enables S3-compatible services. Plain HTTP is rejected unless explicitly
    allowed for local development. Transfer part size is also the adapter's bounded per-upload
    buffering limit.
    """

    bucket: str
    region_name: str | None = None
    endpoint_url: str | None = None
    allow_insecure_http: bool = False
    part_size_bytes: int = 8 * 1024 * 1024
    max_parts: int = 10_000
    read_chunk_size: int = 64 * 1024
    max_pool_connections: int = 50
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        """Reject unsafe or unbounded settings without echoing credential-bearing URLs."""
        if not isinstance(self.bucket, str) or not self.bucket.strip() or len(self.bucket) > 255:
            raise StorageConfigurationError(
                "S3 bucket must be non-empty and at most 255 characters."
            )
        if any(ord(character) < 32 or ord(character) == 127 for character in self.bucket):
            raise StorageConfigurationError("S3 bucket cannot contain control characters.")
        if self.region_name is not None and (
            not isinstance(self.region_name, str)
            or not self.region_name.strip()
            or len(self.region_name) > 128
        ):
            raise StorageConfigurationError("S3 region_name must be non-empty when provided.")
        if not isinstance(self.allow_insecure_http, bool):
            raise StorageConfigurationError("allow_insecure_http must be a boolean.")
        if self.endpoint_url is not None:
            self._validate_endpoint(self.endpoint_url)
        if (
            isinstance(self.part_size_bytes, bool)
            or not isinstance(self.part_size_bytes, int)
            or not _MIN_PART_SIZE <= self.part_size_bytes <= _MAX_PART_SIZE
        ):
            raise StorageConfigurationError("S3 part_size_bytes must be between 5 MiB and 5 GiB.")
        if (
            isinstance(self.max_parts, bool)
            or not isinstance(self.max_parts, int)
            or not 1 <= self.max_parts <= 10_000
        ):
            raise StorageConfigurationError("S3 max_parts must be between 1 and 10000.")
        if (
            isinstance(self.read_chunk_size, bool)
            or not isinstance(self.read_chunk_size, int)
            or not 1 <= self.read_chunk_size <= 16 * 1024 * 1024
        ):
            raise StorageConfigurationError("S3 read_chunk_size must be between 1 byte and 16 MiB.")
        if (
            isinstance(self.max_pool_connections, bool)
            or not isinstance(self.max_pool_connections, int)
            or not 1 <= self.max_pool_connections <= 10_000
        ):
            raise StorageConfigurationError("max_pool_connections must be between 1 and 10000.")
        for name, value in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("read_timeout_seconds", self.read_timeout_seconds),
        ):
            try:
                valid = (
                    isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and math.isfinite(value)
                    and 0 < value <= 300
                )
            except OverflowError:
                valid = False
            if not valid:
                raise StorageConfigurationError(f"{name} must be finite and between 0 and 300.")

    def _validate_endpoint(self, endpoint: str) -> None:
        """Require a safe HTTP(S) endpoint without userinfo, query, or fragment data."""
        try:
            if not isinstance(endpoint, str) or not endpoint.strip():
                raise ValueError
            parsed = urlsplit(endpoint)
            valid = (
                parsed.scheme in {"https", "http"}
                and bool(parsed.hostname)
                and parsed.username is None
                and parsed.password is None
                and not parsed.query
                and not parsed.fragment
                and (parsed.scheme == "https" or self.allow_insecure_http)
            )
        except ValueError:
            valid = False
        if not valid:
            raise StorageConfigurationError("S3 endpoint_url must be a valid secure HTTP(S) URL.")


__all__ = ["S3Config"]
