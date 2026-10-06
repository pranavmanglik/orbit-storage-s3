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
"""Fake-client tests for S3 mapping, streaming, and Core lifecycle ownership."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from orbit import Application, ApplicationConfig
from orbit_storage import (
    OBJECT_STORE_DEPENDENCY_KEY,
    StorageConfigurationError,
    StorageOperationError,
)

from orbit_storage_s3 import S3Config, S3ObjectStore, S3ObjectStorePlugin

_MIN_PART = 5 * 1024 * 1024


class FakeClientError(Exception):
    """Provider-shaped error response used to exercise not-found translation."""

    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class FakeBody:
    """Small async response body with close-state observation."""

    def __init__(self, value: bytes) -> None:
        self.value = value
        self.closed = False
        self.entered = False

    async def read(self, amount: int = -1) -> bytes:
        chunk, self.value = self.value[:amount], self.value[amount:]
        return chunk

    async def __aenter__(self) -> FakeBody:
        self.entered = True
        return self

    async def __aexit__(self, *_: object) -> None:
        assert self.entered
        self.closed = True


class FakeClient:
    """Minimal S3 surface with in-memory values and multipart-call recording."""

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.multipart: dict[str, bytearray] = {}
        self.aborted: list[str] = []
        self.closed = False
        self.last_body: FakeBody | None = None

    async def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects[kwargs["Key"]] = {
            "body": kwargs["Body"],
            "ContentType": kwargs.get("ContentType"),
            "Metadata": kwargs.get("Metadata", {}),
            "ETag": '"etag"',
            "LastModified": datetime.now(UTC),
        }
        return {"ETag": '"etag"'}

    async def get_object(self, **kwargs: Any) -> dict[str, Any]:
        value = self.objects.get(kwargs["Key"])
        if value is None:
            raise FakeClientError("NoSuchKey")
        self.last_body = FakeBody(value["body"])
        return {
            "Body": self.last_body,
            "ContentLength": len(value["body"]),
            "ContentType": value["ContentType"],
            "ETag": value["ETag"],
            "LastModified": value["LastModified"],
            "Metadata": value["Metadata"],
        }

    async def head_object(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs["Key"] not in self.objects:
            raise FakeClientError("404")
        value = self.objects[kwargs["Key"]]
        return {"ContentLength": len(value["body"])}

    async def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.objects.pop(kwargs["Key"], None)
        return {}

    async def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        keys = sorted(key for key in self.objects if key.startswith(kwargs["Prefix"]))
        start = int(kwargs.get("ContinuationToken", "0"))
        page = keys[start : start + kwargs["MaxKeys"]]
        end = start + len(page)
        return {
            "Contents": [
                {
                    "Key": key,
                    "Size": len(self.objects[key]["body"]),
                    "ETag": self.objects[key]["ETag"],
                    "LastModified": self.objects[key]["LastModified"],
                }
                for key in page
            ],
            "IsTruncated": end < len(keys),
            "NextContinuationToken": str(end) if end < len(keys) else None,
        }

    async def create_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.multipart[kwargs["Key"]] = bytearray()
        return {"UploadId": kwargs["Key"]}

    async def upload_part(self, **kwargs: Any) -> dict[str, Any]:
        self.multipart[kwargs["Key"]].extend(kwargs["Body"])
        return {"ETag": f'"part-{kwargs["PartNumber"]}"'}

    async def complete_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        value = bytes(self.multipart.pop(kwargs["Key"]))
        await self.put_object(Bucket=kwargs["Bucket"], Key=kwargs["Key"], Body=value)
        return {"ETag": '"multipart"'}

    async def abort_multipart_upload(self, **kwargs: Any) -> dict[str, Any]:
        self.aborted.append(kwargs["UploadId"])
        self.multipart.pop(kwargs["Key"], None)
        return {}


class FakeClientContext:
    """Context wrapper that records SDK client lifecycle closure."""

    def __init__(self, client: FakeClient) -> None:
        self.client = client

    async def __aenter__(self) -> FakeClient:
        return self.client

    async def __aexit__(self, *_: object) -> None:
        self.client.closed = True


class FakeSession:
    """Inject a fake client context in place of an AWS endpoint."""

    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def create_client(self, service_name: str, **kwargs: Any) -> FakeClientContext:
        assert service_name == "s3"
        assert kwargs["config"].max_pool_connections > 0
        return FakeClientContext(self.client)


def make_store(client: FakeClient, **config: Any) -> S3ObjectStore:
    """Construct an adapter with deterministic settings and fake SDK session."""
    options: dict[str, Any] = {"bucket": "orbit-test", **config}
    if "part_size_bytes" not in options:
        options["part_size_bytes"] = _MIN_PART
    return S3ObjectStore(S3Config(**options), session_factory=lambda: FakeSession(client))


def test_s3_config_rejects_insecure_endpoint_and_unbounded_transfer() -> None:
    with pytest.raises(StorageConfigurationError, match="secure HTTP"):
        S3Config(bucket="b", endpoint_url="http://store.local")
    with pytest.raises(StorageConfigurationError, match="part_size_bytes"):
        S3Config(bucket="b", part_size_bytes=1024)
    assert S3Config(
        bucket="b", endpoint_url="http://127.0.0.1:9000", allow_insecure_http=True
    ).endpoint_url.startswith("http://")


@pytest.mark.asyncio
async def test_put_get_and_delete_follow_storage_contract() -> None:
    client = FakeClient()
    store = make_store(client)
    info = await store.put(
        "reports/today.json", b"hello", content_type="application/json", metadata={"team": "ops"}
    )
    assert info.size_bytes == 5
    assert info.etag == '"etag"'
    stored = await store.get("reports/today.json")
    assert stored is not None
    async with stored:
        assert [part async for part in stored.body] == [b"hello"]
    assert client.last_body is not None and client.last_body.closed
    assert await store.delete("reports/today.json")
    assert not await store.delete("reports/today.json")
    assert await store.get("missing") is None
    await store.aclose()
    assert client.closed


@pytest.mark.asyncio
async def test_async_upload_uses_bounded_multipart_and_completes() -> None:
    client = FakeClient()
    store = make_store(client)

    async def chunks() -> AsyncIterator[bytes]:
        yield b"a" * _MIN_PART
        yield b"tail"

    info = await store.put("large.bin", chunks())
    assert info.size_bytes == _MIN_PART + 4
    assert client.objects["large.bin"]["body"] == b"a" * _MIN_PART + b"tail"
    assert "large.bin" not in client.multipart
    await store.aclose()


@pytest.mark.asyncio
async def test_stream_failure_aborts_multipart_and_preserves_source_error() -> None:
    client = FakeClient()
    store = make_store(client)

    async def broken_chunks() -> AsyncIterator[bytes]:
        yield b"a" * _MIN_PART
        raise RuntimeError("source disconnected")

    with pytest.raises(RuntimeError, match="source disconnected"):
        await store.put("interrupted.bin", broken_chunks())
    assert client.aborted == ["interrupted.bin"]
    assert client.multipart == {}
    await store.aclose()


@pytest.mark.asyncio
async def test_sdk_failure_is_sanitized() -> None:
    class FailingClient(FakeClient):
        async def put_object(self, **kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("access key: sensitive-token")

    store = make_store(FailingClient())
    with pytest.raises(StorageOperationError) as captured:
        await store.put("safe-key", b"payload")
    assert "sensitive-token" not in str(captured.value)
    await store.aclose()


@pytest.mark.asyncio
async def test_list_page_and_plugin_lifecycle() -> None:
    client = FakeClient()
    store = make_store(client)
    await store.put("files/a", b"a")
    await store.put("files/b", b"b")
    page = await store.list_page("files/", limit=1)
    assert [item.key for item in page.items] == ["files/a"]
    assert page.continuation_token == "1"
    app = Application(ApplicationConfig(name="s3-plugin-test"))
    app.plugins.register(S3ObjectStorePlugin(store))
    await app.startup()
    assert await app.container.aresolve(OBJECT_STORE_DEPENDENCY_KEY) is store
    await app.stop()
    assert client.closed
