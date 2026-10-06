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
"""Amazon S3 implementation of Orbit Storage's asynchronous object contract."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Mapping
from contextlib import AsyncExitStack, suppress
from typing import Any, Protocol, cast

from aiobotocore.config import AioConfig  # type: ignore[import-untyped]
from aiobotocore.session import get_session  # type: ignore[import-untyped]
from orbit_storage import (
    ListObjectsRequest,
    ObjectInfo,
    ObjectPage,
    ObjectStore,
    StorageConfigurationError,
    StorageOperationError,
    StoredObject,
    validate_object_key,
)

from orbit_storage_s3.config import S3Config


class _S3Client(Protocol):
    """Subset of the generated aiobotocore S3 client used by the adapter."""

    async def put_object(self, **kwargs: Any) -> dict[str, Any]:
        """Create an object from byte content and return S3 metadata."""

    async def get_object(self, **kwargs: Any) -> dict[str, Any]:
        """Open an object and return its response body and metadata."""

    async def head_object(self, **kwargs: Any) -> dict[str, Any]:
        """Read object headers without downloading its body."""

    async def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        """Request object deletion."""

    async def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        """List one bounded page of object keys."""

    async def create_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        """Start a multipart upload and return its provider upload identifier."""

    async def upload_part(self, **kwargs: Any) -> dict[str, Any]:
        """Send one numbered multipart body and return its entity tag."""

    async def complete_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        """Complete multipart assembly from the ordered uploaded parts."""

    async def abort_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        """Abort unfinished multipart work and release uploaded parts."""


class _Body(Protocol):
    """Async S3 response body and context-manager cleanup interface."""

    async def read(self, amount: int = -1) -> bytes:
        """Read the requested number of bytes from the HTTP response."""

    async def __aenter__(self) -> _Body: ...

    async def __aexit__(self, *args: object) -> None: ...


class _ClientContext(Protocol):
    """Context returned by aiobotocore's S3 client factory."""

    async def __aenter__(self) -> _S3Client: ...

    async def __aexit__(self, *args: object) -> None: ...


class _Session(Protocol):
    """Session interface kept narrow for deterministic adapter tests."""

    def create_client(self, service_name: str, **kwargs: Any) -> _ClientContext:
        """Return an async-managed SDK client for the named AWS service."""


class _S3BodyReader:
    """Adapt aiobotocore's closeable StreamingBody to Orbit's async byte reader."""

    def __init__(self, body: _Body, chunk_size: int) -> None:
        """Take ownership of the response stream returned by GetObject."""
        self._body = body
        self._chunk_size = chunk_size
        self._closed = False

    def __aiter__(self) -> _S3BodyReader:
        """Return this closeable asynchronous body reader."""
        return self

    async def __anext__(self) -> bytes:
        """Read one bounded chunk and close the provider stream at EOF."""
        if self._closed:
            raise StopAsyncIteration
        try:
            chunk = await self._body.read(self._chunk_size)
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.aclose()
            raise StorageOperationError(
                "S3 object download failed while reading the body."
            ) from None
        if not chunk:
            await self.aclose()
            raise StopAsyncIteration
        if not isinstance(chunk, bytes):
            await self.aclose()
            raise StorageOperationError("S3 response returned a non-byte object body.")
        return chunk

    async def aclose(self) -> None:
        """Close the StreamingBody context once, releasing its HTTP connection."""
        if not self._closed:
            try:
                await self._body.__aexit__(None, None, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise StorageOperationError("S3 response stream close failed.") from None
            self._closed = True


class S3ObjectStore(ObjectStore):
    """Lazy, lifecycle-managed S3 object store with bounded multipart buffering."""

    def __init__(self, config: S3Config, *, session_factory: Any | None = None) -> None:
        """Bind settings; use aiobotocore's standard credentials unless tests inject a session."""
        if not isinstance(config, S3Config):
            raise TypeError("S3ObjectStore requires an S3Config instance.")
        self.config = config
        self._session_factory = session_factory or get_session
        self._stack = AsyncExitStack()
        self._client: _S3Client | None = None
        self._client_lock = asyncio.Lock()
        self._closed = False

    async def _get_client(self) -> _S3Client:
        """Create and retain one async S3 client until explicit close or plugin shutdown."""
        async with self._client_lock:
            if self._closed:
                raise StorageOperationError("S3 object store is closed.")
            if self._client is None:
                try:
                    session = cast(_Session, self._session_factory())
                    options: dict[str, Any] = {
                        "config": AioConfig(
                            connect_timeout=self.config.connect_timeout_seconds,
                            read_timeout=self.config.read_timeout_seconds,
                            max_pool_connections=self.config.max_pool_connections,
                        )
                    }
                    if self.config.region_name is not None:
                        options["region_name"] = self.config.region_name
                    if self.config.endpoint_url is not None:
                        options["endpoint_url"] = self.config.endpoint_url
                    self._client = await self._stack.enter_async_context(
                        session.create_client("s3", **options)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise StorageOperationError("S3 client initialization failed.") from None
            return self._client

    def _key(self, key: str) -> str:
        """Apply the capability's opaque bounded-key policy before provider calls."""
        try:
            return validate_object_key(key)
        except ValueError:
            raise StorageConfigurationError("S3 object key is invalid.") from None

    @staticmethod
    def _not_found(error: Exception) -> bool:
        """Recognize only S3's documented missing-object response codes."""
        response = getattr(error, "response", None)
        if not isinstance(response, Mapping):
            return False
        details = response.get("Error")
        if not isinstance(details, Mapping):
            return False
        return details.get("Code") in {"NoSuchKey", "NotFound", "404"}

    @staticmethod
    def _info(
        key: str,
        *,
        size: Any,
        content_type: str | None = None,
        etag: str | None = None,
        modified_at: Any = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        """Normalize S3 response fields through the capability's runtime model."""
        try:
            return ObjectInfo(
                key=key,
                size_bytes=size,
                content_type=content_type,
                etag=etag,
                modified_at=modified_at,
                metadata=metadata or {},
            )
        except Exception:
            raise StorageOperationError("S3 returned invalid object metadata.") from None

    async def put(
        self,
        key: str,
        body: bytes | AsyncIterable[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        """Create or replace one object, using multipart upload for streamed bodies."""
        object_key = self._key(key)
        validated = self._info(
            object_key, size=0, content_type=content_type, metadata=metadata or {}
        )
        if isinstance(body, bytes):
            return await self._put_bytes(
                object_key, body, validated.content_type, validated.metadata
            )
        if not hasattr(body, "__aiter__"):
            raise StorageConfigurationError("S3 upload body must be bytes or an async byte stream.")
        return await self._put_stream(object_key, body, validated.content_type, validated.metadata)

    async def _put_bytes(
        self,
        key: str,
        body: bytes,
        content_type: str | None,
        metadata: Mapping[str, str],
    ) -> ObjectInfo:
        """Use single-request PutObject for already-materialized byte content."""
        client = await self._get_client()
        arguments: dict[str, Any] = {"Bucket": self.config.bucket, "Key": key, "Body": body}
        if content_type is not None:
            arguments["ContentType"] = content_type
        if metadata:
            arguments["Metadata"] = dict(metadata)
        try:
            response = await client.put_object(**arguments)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError("S3 put_object operation failed.") from None
        return self._info(
            key,
            size=len(body),
            content_type=content_type,
            etag=response.get("ETag"),
            metadata=metadata,
        )

    async def _put_stream(
        self,
        key: str,
        body: AsyncIterable[bytes],
        content_type: str | None,
        metadata: Mapping[str, str],
    ) -> ObjectInfo:
        """Upload an async stream in sequential bounded parts; abort on every failed exit."""
        client = await self._get_client()
        iterator = aiter(body)
        pending = bytearray()
        source_chunk = b""
        source_offset = 0
        exhausted = False

        async def read_part() -> bytes:
            nonlocal exhausted, source_chunk, source_offset
            while len(pending) < self.config.part_size_bytes and not exhausted:
                if source_offset == len(source_chunk):
                    try:
                        source_chunk = await anext(iterator)
                    except StopAsyncIteration:
                        exhausted = True
                        break
                    if not isinstance(source_chunk, bytes):
                        raise StorageConfigurationError("S3 upload streams must yield byte chunks.")
                    source_offset = 0
                    if not source_chunk:
                        await asyncio.sleep(0)
                        continue
                needed = self.config.part_size_bytes - len(pending)
                available = len(source_chunk) - source_offset
                amount = min(needed, available)
                pending.extend(source_chunk[source_offset : source_offset + amount])
                source_offset += amount
            part = bytes(pending[: self.config.part_size_bytes])
            del pending[: len(part)]
            return part

        first = await read_part()
        if exhausted:
            return await self._put_bytes(key, first, content_type, metadata)

        arguments: dict[str, Any] = {"Bucket": self.config.bucket, "Key": key}
        if content_type is not None:
            arguments["ContentType"] = content_type
        if metadata:
            arguments["Metadata"] = dict(metadata)
        try:
            created = await client.create_multipart_upload(**arguments)
            upload_id = created["UploadId"]
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError("S3 multipart upload initialization failed.") from None

        parts: list[dict[str, Any]] = []
        total_size = 0
        current = first
        try:
            while current:
                if len(parts) >= self.config.max_parts:
                    raise StorageConfigurationError(
                        "S3 streamed upload exceeded the configured multipart part limit."
                    )
                part_number = len(parts) + 1
                try:
                    uploaded = await client.upload_part(
                        Bucket=self.config.bucket,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=current,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise StorageOperationError("S3 upload_part operation failed.") from None
                parts.append({"PartNumber": part_number, "ETag": uploaded["ETag"]})
                total_size += len(current)
                current = b""
                current = await read_part()
            try:
                completed = await client.complete_multipart_upload(
                    Bucket=self.config.bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise StorageOperationError(
                    "S3 complete_multipart_upload operation failed."
                ) from None
        except BaseException:
            await self._abort(client, key, upload_id)
            raise
        return self._info(
            key,
            size=total_size,
            content_type=content_type,
            etag=completed.get("ETag"),
            metadata=metadata,
        )

    async def _abort(self, client: _S3Client, key: str, upload_id: str) -> None:
        """Best-effort cleanup that preserves cancellation and the original failure."""
        task = asyncio.create_task(
            client.abort_multipart_upload(Bucket=self.config.bucket, Key=key, UploadId=upload_id)
        )
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            with suppress(Exception, asyncio.CancelledError):
                await task
        except Exception:
            pass

    async def get(self, key: str) -> StoredObject | None:
        """Return metadata and a closeable, incrementally-read S3 response body."""
        object_key = self._key(key)
        client = await self._get_client()
        try:
            response = await client.get_object(Bucket=self.config.bucket, Key=object_key)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._not_found(error):
                return None
            raise StorageOperationError("S3 get_object operation failed.") from None
        body = cast(_Body, response.get("Body"))
        try:
            stream = await body.__aenter__()
            info = self._info(
                object_key,
                size=response.get("ContentLength"),
                content_type=response.get("ContentType"),
                etag=response.get("ETag"),
                modified_at=response.get("LastModified"),
                metadata=response.get("Metadata", {}),
            )
        except asyncio.CancelledError:
            with suppress(Exception):
                await body.__aexit__(None, None, None)
            raise
        except Exception:
            with suppress(Exception):
                await body.__aexit__(None, None, None)
            raise StorageOperationError("S3 returned an invalid object response.") from None
        reader = _S3BodyReader(stream, self.config.read_chunk_size)
        return StoredObject(info=info, body=reader)

    async def delete(self, key: str) -> bool:
        """Delete an object and report whether a preceding existence check found it."""
        object_key = self._key(key)
        client = await self._get_client()
        try:
            await client.head_object(Bucket=self.config.bucket, Key=object_key)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._not_found(error):
                return False
            raise StorageOperationError("S3 head_object operation failed.") from None
        try:
            await client.delete_object(Bucket=self.config.bucket, Key=object_key)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError("S3 delete_object operation failed.") from None
        return True

    async def list_page(
        self,
        prefix: str = "",
        *,
        limit: int = 100,
        continuation_token: str | None = None,
    ) -> ObjectPage:
        """List one page using S3's opaque ListObjectsV2 continuation token."""
        try:
            request = ListObjectsRequest(
                prefix=prefix, limit=limit, continuation_token=continuation_token
            )
        except Exception:
            raise StorageConfigurationError("S3 list request is invalid.") from None
        client = await self._get_client()
        arguments: dict[str, Any] = {
            "Bucket": self.config.bucket,
            "Prefix": request.prefix,
            "MaxKeys": request.limit,
        }
        if request.continuation_token is not None:
            arguments["ContinuationToken"] = request.continuation_token
        try:
            response = await client.list_objects_v2(**arguments)
            contents = response.get("Contents", [])
            items = tuple(
                self._info(
                    str(item["Key"]),
                    size=item.get("Size"),
                    etag=item.get("ETag"),
                    modified_at=item.get("LastModified"),
                )
                for item in contents
            )
            token = response.get("NextContinuationToken") if response.get("IsTruncated") else None
            if response.get("IsTruncated") and not token:
                raise ValueError("Truncated S3 page is missing its continuation token.")
            return ObjectPage(items=items, continuation_token=token)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError("S3 list_objects_v2 operation failed.") from None

    async def aclose(self) -> None:
        """Close the retained aiobotocore client; repeated calls are safe."""
        async with self._client_lock:
            if self._closed:
                return
            self._closed = True
            try:
                await self._stack.aclose()
            except asyncio.CancelledError:
                self._closed = False
                raise
            except Exception:
                raise StorageOperationError("S3 client shutdown failed.") from None


__all__ = ["S3ObjectStore"]
