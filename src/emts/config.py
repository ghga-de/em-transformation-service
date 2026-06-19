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

"""Config Parameter Modeling and Parsing."""

from pathlib import Path

from hexkit.config import config_from_yaml
from hexkit.log import LoggingConfig
from hexkit.providers.mongokafka import MongoKafkaConfig
from pydantic import Field

from emts.adapters.inbound.event_sub import AEMPackTranslatorConfig
from emts.adapters.outbound.dao import AEMPackDaoConfig

SERVICE_NAME: str = "emts"


@config_from_yaml(prefix=SERVICE_NAME)
class Config(
    LoggingConfig, MongoKafkaConfig, AEMPackTranslatorConfig, AEMPackDaoConfig
):
    """Config parameters and their defaults."""

    service_name: str = Field(
        default=SERVICE_NAME, description="Short name of this service"
    )
    processing_configuration_path: Path = Field(
        default=...,
        description="Path to the config file used to populate the database with models, transformations and workflows.",
    )
    processing_poll_pause: int = Field(
        default=60,
        description="Seconds to sleep when no unprocessed AEMPacks are found.",
    )
    worker_id: str = Field(
        default=...,
        description="Unique identifier for a service instance used specifically for the reclamation logic.",
    )
    config_lock_expiry_seconds: int = Field(
        default=120,
        description="TTL in seconds for the config lock document. MongoDB automatically removes stale locks after this duration.",
    )
    config_lock_poll_interval: int = Field(
        default=5,
        description="Seconds between polls when waiting for the config lock to be released.",
    )
    config_lock_timeout: int = Field(
        default=150,
        description="Maximum seconds to wait for the config lock to be released before raising a TimeoutError.",
    )
