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
"""Models for KafkaEventSubscriber"""

from pydantic import Field
from pydantic_settings import BaseSettings


class AEMPackEventConfig(BaseSettings):
    """This will go to the event schemas.stateful?."""

    aem_pack_upsert_topic: str = Field(
        default=...,
        description="Name of the topic used for events indicating that an original"
        " AEMPack is registered for transformation",
    )
