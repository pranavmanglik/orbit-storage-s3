# Orbit Storage S3: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-storage-s3` implements the `orbit-storage` `ObjectStore` capability using aiobotocore. Install it
only when an application uses S3 or an S3-compatible service:

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
default) and abort incomplete multipart work after failure or cancellation. A per-store semaphore
limits streamed upload concurrency (`max_concurrent_stream_uploads`, 4 by default). Their product
cannot exceed 64 MiB, and each part is capped at 64 MiB. This bounds Orbit's multipart buffers but
does not account for memory already held by a caller's source chunk or by the SDK/HTTP stack. The
maximum streamed object size is `part_size_bytes * max_parts`; the defaults do not claim S3's full
maximum object size. Downloads hold one of `max_concurrent_downloads` slots (16 by default, at most
256) for the full response lifetime. `read_chunk_size * max_concurrent_downloads` is capped at 64
MiB for Orbit-requested response chunks. Close each `StoredObject` before store shutdown and include
provider and HTTP buffers in the deployment's memory budget.

The AWS client explicitly selects standard retries with jittered exponential backoff.
`total_max_attempts` defaults to 3, counting the initial attempt, and is bounded from 1 through 10.
Use `1` to disable retries. The SDK retries only failures it classifies as retryable; verify the
selected S3-compatible provider's status and error-code behavior separately. See the
[AWS retry behavior guide](https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html).

`delete` checks object existence before issuing `DeleteObject` to implement the capability's
boolean result; concurrent deletion can race that check. S3 versioning, retention, encryption,
conditional writes, bucket policy, and IAM remain operator/provider concerns. Credentials and raw
SDK exception text are not included in normalized storage errors. The Core plugin registers under
`OBJECT_STORE_DEPENDENCY_KEY` and closes the client at application shutdown. No live AWS or
S3-compatible endpoint test is claimed; tests use fake client objects.

`aclose()` makes the store unavailable before it starts cleanup. If the SDK context raises or the
call is cancelled, the context remains owned by the store and a later `aclose()` retries cleanup;
once cleanup succeeds, additional calls are harmless. The host should log/report a failed shutdown
and decide whether its termination deadline allows another cleanup attempt.

The adapter follows the official [aiobotocore client and StreamingBody patterns](https://github.com/aio-libs/aiobotocore)
and AWS [ListObjectsV2 continuation-token contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
