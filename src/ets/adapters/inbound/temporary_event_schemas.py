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

"""Temporary models and config for KafkaEventSubscriber to be replaced by ghga_event_schemas."""

from pydantic import Field
from pydantic_settings import BaseSettings

from ets.core.models import AEMPack


class AEMPackEventConfig(BaseSettings):
    """Config for events communicating changes in AEMPacks.

    The event types are hardcoded by `hexkit`.
    """

    original_aem_pack_topic: str = Field(
        default=...,
        description="Topic informing about new ingress AEMPacks.",
        examples=["original-aempacks"],
    )


class OriginalAEMPack(AEMPack):
    """Model for derived AEMPacks."""
