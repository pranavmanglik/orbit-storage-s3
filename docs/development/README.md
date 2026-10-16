# Orbit Storage S3: development

This package declares support for `>=3.11,<3.15` in `pyproject.toml`. That declaration is distinct from evidence that hosted CI ran.

Use the checks for each language manifest present in this repository:

### Python package

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
```

Optional SDKs and live-service tests are described by the package README. Do not run an opt-in live test against a shared or production service unless its own documentation explicitly supports that use.

## Contribution expectations

Add focused regression coverage for behavior changes. Update the README and relevant guide when a contract, configuration option, security property, lifecycle rule, or operational assumption changes. Report the exact versions and services exercised; local checks do not imply hosted CI or release validation.

The previously recorded local quality matrix passed on CPython 3.11, 3.12, 3.13, and 3.14 with
7 tests, Ruff lint and format checks, strict mypy, and wheel/sdist builds on each interpreter. The
shutdown-retry regression added since then passed in the current CPython 3.13.15 check; it has not
yet been rerun across the full interpreter matrix. Tests use injected clients and do not verify AWS
IAM, a specific S3-compatible service, network performance, or hosted CI.

Lifecycle regression coverage includes client contexts whose first close fails or is cancelled and
whose later close succeeds. These checks verify adapter ownership and retry behavior with fake SDK
contexts; they do not establish how a specific provider behaves during network or process failure.
