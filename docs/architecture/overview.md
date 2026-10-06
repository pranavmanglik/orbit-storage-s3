# Orbit Storage S3: architecture and boundaries

## Responsibility

`orbit-storage-s3` implements the `orbit-storage` `ObjectStore` capability using aiobotocore. Install it
only when an application uses S3 or an S3-compatible service:

```bash
python -m pip install orbit-core orbit-storage orbit-storage-s3
```

Configure credentials through aiobotocore's standard AWS credential chain (environment, shared
configuration, workload identity/role credentials, or another supported provider). Orbit does not
copy credentials into its own configuration model. HTTPS is required for custom endpoints unless
`allow_insecure_http=True` is explicitly selected for local development. Configure `endpoint_url`
for a compatible object service; endpoint-specific behavior remains outside Core.

```python
from orbit import Application, ApplicationConfig
from orbit_storage_s3 import S3Config, S3ObjectStore, S3ObjectStorePlugin

application = Application(ApplicationConfig(name="reports"))
store = S3ObjectStore(S3Config(bucket="orbit-reports", region_name="us-east-1"))
application.plugins.register(S3ObjectStorePlugin(store))
await application.startup()

await store.put("daily/report.json", b'{"ready": true}', content_type="application/json")
download = await store.get("daily/report.json")
if download is not None:
    async with download:
        async for chunk in download.body:
            consume(chunk)
```

The adapter maps keys and normalized metadata directly to the selected bucket, implements
`ListObjectsV2` pagination, and streams downloads from aiobotocore's response body. Streamed uploads
use sequential S3 multipart requests with bounded adapter buffering (`part_size_bytes`, 8 MiB by
default, plus at most one source chunk) and abort incomplete multipart work after failure or
cancellation. The maximum streamed object size is
bounded by `part_size_bytes * max_parts`; increase the part size deliberately when larger objects
are required, considering the corresponding per-upload memory use. This does not claim S3's full
maximum object size for the default configuration.

`delete` checks object existence before issuing `DeleteObject` to implement the capability's
boolean result; concurrent deletion can race that check. S3 versioning, retention, encryption,
conditional writes, bucket policy, and IAM remain operator/provider concerns. Credentials and raw
SDK exception text are not included in normalized storage errors. The Core plugin registers under
`OBJECT_STORE_DEPENDENCY_KEY` and closes the client at application shutdown. No live AWS or
S3-compatible endpoint test is claimed; tests use fake client objects.

The adapter follows the official [aiobotocore client and StreamingBody patterns](https://github.com/aio-libs/aiobotocore)
and AWS [ListObjectsV2 continuation-token contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `aiobotocore>=3.8,<4`
- `orbit-core>=0.1.0a1,<0.2`
- `orbit-storage>=0.1.0a1,<0.2`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_storage_s3/__init__.py`
- `src/orbit_storage_s3/config.py`
- `src/orbit_storage_s3/plugin.py`
- `src/orbit_storage_s3/store.py`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
