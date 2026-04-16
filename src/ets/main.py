# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
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

"""Top-level functions for the service"""

from hexkit.log import configure_logging

from ets.config import Config
from ets.inject import prepare_aem_pack_registry, prepare_event_subscriber


async def consume_events(run_forever: bool = True):
    """Run the event consumer"""
    config = Config()  # type: ignore[call-arg]
    configure_logging(config=config)

    async with prepare_event_subscriber(config=config) as event_subscriber:
        await event_subscriber.run(forever=run_forever)


async def process_aem_packs():
    """Run processing on incoming annotated experimental metadata that has been stored in the database."""
    config = Config()  # type: ignore[call-arg]
    configure_logging(config=config)

    async with prepare_aem_pack_registry(config=config) as aem_pack_registry:
        await aem_pack_registry.process_aem_packs()
