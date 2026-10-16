# Orbit Storage S3

`orbit-storage-s3` implements the `orbit-storage` `ObjectStore` capability using aiobotocore. Install it
only when an application uses S3 or an S3-compatible service:
`S3ObjectStore` works directly with `orbit-storage`; `S3ObjectStorePlugin` is an optional Core
integration from `orbit_storage_s3.plugin`.

```bash
python -m pip install orbit-storage orbit-storage-s3
# Add the Core lifecycle plugin only when using it:
python -m pip install 'orbit-storage-s3[core]'
```

Configure credentials through aiobotocore's standard AWS credential chain (environment, shared
configuration, workload identity/role credentials, or another supported provider). Orbit does not
copy credentials into its own configuration model. HTTPS is required for custom endpoints unless
`allow_insecure_http=True` is explicitly selected for local development. Configure `endpoint_url`
for a compatible object service; endpoint-specific behavior remains outside Core.

```python
from orbit import Application, ApplicationConfig
from orbit_storage_s3 import S3Config, S3ObjectStore
from orbit_storage_s3.plugin import S3ObjectStorePlugin

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
use sequential S3 multipart requests with a bounded part buffer (`part_size_bytes`, 8 MiB by
default) and abort incomplete multipart work after failure or cancellation. Streamed uploads are
admitted through a per-store semaphore (`max_concurrent_stream_uploads`, 4 by default); the
configured part size multiplied by that concurrency may not exceed 64 MiB. Part sizes are limited
to 64 MiB. These limits bound Orbit's multipart buffers, excluding memory already held by the
caller's source chunk and the SDK/HTTP stack. The maximum streamed object size is bounded by
`part_size_bytes * max_parts`; this does not claim S3's full maximum object size for the default
configuration.

The async AWS client uses standard SDK retries with jittered exponential backoff for retryable
transient and throttling responses. `total_max_attempts` defaults to 3 (including the initial call)
and can be set from 1 to 10; choose `1` to disable retries. The explicit client setting takes
precedence over environment and shared AWS configuration. See the
[AWS retry behavior guide](https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html).

`delete` checks object existence before issuing `DeleteObject` to implement the capability's
boolean result; concurrent deletion can race that check. S3 versioning, retention, encryption,
conditional writes, bucket policy, and IAM remain operator/provider concerns. Credentials and raw
SDK exception text are not included in normalized storage errors. The Core plugin registers under
`OBJECT_STORE_DEPENDENCY_KEY` and closes the client at application shutdown. No live AWS or
S3-compatible endpoint test is claimed; tests use fake client objects.

The plugin contributes a read-only **Check S3 bucket access** action to Orbit CLI. It lists at most
one object without displaying object names, so the configured identity needs `s3:ListBucket` on the
bucket. The action does not upload, delete, or download object contents.

If client shutdown fails, the store rejects new operations and retains the SDK context so a caller
can retry `aclose()`. A successful close is idempotent. Applications should surface shutdown errors
and include cleanup retries in their host shutdown policy where appropriate.

The adapter follows the official [aiobotocore client and StreamingBody patterns](https://github.com/aio-libs/aiobotocore)
and AWS [ListObjectsV2 continuation-token contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

This package is alpha, its API is not stable, and it supports Python 3.11 through 3.14.
Licensed under Apache-2.0.

