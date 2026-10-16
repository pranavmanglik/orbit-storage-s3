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
"""Optional Core plugin that registers and owns an S3 object-store adapter."""

from collections.abc import Mapping

from orbit import Application
from orbit.plugins import ControlAction, ControlResult, Plugin, PluginMetadata
from orbit_storage import OBJECT_STORE_DEPENDENCY_KEY

from orbit_storage_s3.store import S3ObjectStore


class S3ObjectStorePlugin(Plugin):
    """Register ``ObjectStore`` under the shared capability key and close it on shutdown."""

    metadata = PluginMetadata(
        name="orbit-storage-s3",
        version="0.1.0a1",
        capabilities=frozenset({"storage.s3"}),
    )

    def __init__(self, store: S3ObjectStore) -> None:
        """Take lifecycle ownership of an explicitly configured adapter instance."""
        if not isinstance(store, S3ObjectStore):
            raise TypeError("S3ObjectStorePlugin requires an S3ObjectStore instance.")
        self.store = store
        self.control_actions = (
            ControlAction(
                id="check-connection",
                title="Check S3 bucket access",
                description="List at most one object to verify read access to the bucket.",
                handler=self._check_connection,
            ),
        )

    def setup(self, application: Application) -> None:
        """Expose the adapter through the provider-neutral storage dependency key."""
        if not isinstance(application, Application):
            raise TypeError("S3ObjectStorePlugin requires an Orbit Application.")
        application.container.register_instance(OBJECT_STORE_DEPENDENCY_KEY, self.store)

    async def deactivate(self) -> None:
        """Close the SDK client during normal shutdown or partial-startup rollback."""
        await self.store.aclose()

    async def _check_connection(self, _: Mapping[str, object]) -> ControlResult:
        """Issue one bounded read-only list request without displaying object names."""
        await self.store.list_page(limit=1)
        return ControlResult("S3 bucket listing is available.")


__all__ = ["S3ObjectStorePlugin"]
