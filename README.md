[![tests](https://github.com/ghga-de/em-transformation-service/actions/workflows/tests.yaml/badge.svg)](https://github.com/ghga-de/em-transformation-service/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/em-transformation-service/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/em-transformation-service?branch=main)

# Em Transformation Service

EM-Transformation-Service - a service that transforms Experimental Metadata

## Description

<!-- Please provide a short overview of the features of this service. -->

Here you should provide a short summary of the purpose of this microservice.


## Installation

We recommend using the provided Docker container.

A pre-built version is available on [Docker Hub](https://hub.docker.com/repository/docker/ghga/em-transformation-service):
```bash
docker pull ghga/em-transformation-service:0.1.0
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/em-transformation-service:0.1.0 .
```

For production-ready deployment, we recommend using Kubernetes.
However for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is pre-configured:
docker run -p 8080:8080 ghga/em-transformation-service:0.1.0 --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
emts --help
```

## Configuration

### Parameters

The service requires the following configuration parameters:
- <a id="properties/derived_aem_pack_topic"></a>**`derived_aem_pack_topic`** *(string, required)*: Topic for events informing about derived AEMPacks.

  Examples:
  ```json
  "derived-aempacks"
  ```

- <a id="properties/aem_pack_processing_status_topic"></a>**`aem_pack_processing_status_topic`** *(string, required)*: Topic for AEMPack processing-lifecycle (status) events, e.g. processing failures, and later successes.

  Examples:
  ```json
  "aempack-processing-status"
  ```

- <a id="properties/original_aem_pack_topic"></a>**`original_aem_pack_topic`** *(string, required)*: Topic informing about new ingress AEMPacks.

  Examples:
  ```json
  "original-aempacks"
  ```

- <a id="properties/service_name"></a>**`service_name`** *(string)*: Short name of this service. Default: `"emts"`.
- <a id="properties/service_instance_id"></a>**`service_instance_id`** *(string, required)*: A string that uniquely identifies this instance across all instances of this service. This is included in log messages.

  Examples:
  ```json
  "germany-bw-instance-001"
  ```

- <a id="properties/kafka_servers"></a>**`kafka_servers`** *(array, required)*: A list of connection strings to connect to Kafka bootstrap servers.
  - <a id="properties/kafka_servers/items"></a>**Items** *(string)*

  Examples:
  ```json
  [
      "localhost:9092"
  ]
  ```

- <a id="properties/kafka_security_protocol"></a>**`kafka_security_protocol`** *(string)*: Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL. Must be one of: "PLAINTEXT" or "SSL". Default: `"PLAINTEXT"`.
- <a id="properties/kafka_ssl_cafile"></a>**`kafka_ssl_cafile`** *(string)*: Certificate Authority file path containing certificates used to sign broker certificates. If a CA is not specified, the default system CA will be used if found by OpenSSL. Default: `""`.
- <a id="properties/kafka_ssl_certfile"></a>**`kafka_ssl_certfile`** *(string)*: Optional filename of client certificate, as well as any CA certificates needed to establish the certificate's authenticity. Default: `""`.
- <a id="properties/kafka_ssl_keyfile"></a>**`kafka_ssl_keyfile`** *(string)*: Optional filename containing the client private key. Default: `""`.
- <a id="properties/kafka_ssl_password"></a>**`kafka_ssl_password`** *(string, format: password, write-only)*: Optional password to be used for the client private key. Default: `""`.
- <a id="properties/generate_correlation_id"></a>**`generate_correlation_id`** *(boolean)*: A flag, which, if False, will result in an error when trying to publish an event without a valid correlation ID set for the context. If True, a new correlation ID will be generated and used in the event header. Default: `true`.

  Examples:
  ```json
  true
  ```

  ```json
  false
  ```

- <a id="properties/kafka_max_message_size"></a>**`kafka_max_message_size`** *(integer)*: The largest message size that can be transmitted, in bytes, before compression. Only services that have a need to send/receive larger messages should set this. When used alongside compression, this value can be set to something greater than the broker's `message.max.bytes` field, which effectively concerns the compressed message size. Exclusive minimum: `0`. Default: `1048576`.

  Examples:
  ```json
  1048576
  ```

  ```json
  16777216
  ```

- <a id="properties/kafka_compression_type"></a>**`kafka_compression_type`**: The compression type used for messages. Valid values are: None, gzip, snappy, lz4, and zstd. If None, no compression is applied. This setting is only relevant for the producer and has no effect on the consumer. If set to a value, the producer will compress messages before sending them to the Kafka broker. If unsure, zstd provides a good balance between speed and compression ratio. Default: `null`.
  - **Any of**
    - <a id="properties/kafka_compression_type/anyOf/0"></a>*string*: Must be one of: "gzip", "snappy", "lz4", or "zstd".
    - <a id="properties/kafka_compression_type/anyOf/1"></a>*null*

  Examples:
  ```json
  null
  ```

  ```json
  "gzip"
  ```

  ```json
  "snappy"
  ```

  ```json
  "lz4"
  ```

  ```json
  "zstd"
  ```

- <a id="properties/kafka_max_retries"></a>**`kafka_max_retries`** *(integer)*: The maximum number of times to immediately retry consuming an event upon failure. Works independently of the dead letter queue. Minimum: `0`. Default: `0`.

  Examples:
  ```json
  0
  ```

  ```json
  1
  ```

  ```json
  2
  ```

  ```json
  3
  ```

  ```json
  5
  ```

- <a id="properties/kafka_enable_dlq"></a>**`kafka_enable_dlq`** *(boolean)*: A flag to toggle the dead letter queue. If set to False, the service will crash upon exhausting retries instead of publishing events to the DLQ. If set to True, the service will publish events to the DLQ topic after exhausting all retries. Default: `false`.

  Examples:
  ```json
  true
  ```

  ```json
  false
  ```

- <a id="properties/kafka_dlq_topic"></a>**`kafka_dlq_topic`** *(string)*: The name of the topic used to resolve error-causing events. Default: `"dlq"`.

  Examples:
  ```json
  "dlq"
  ```

- <a id="properties/kafka_retry_backoff"></a>**`kafka_retry_backoff`** *(integer)*: The number of seconds to wait before retrying a failed event. The backoff time is doubled for each retry attempt. Minimum: `0`. Default: `0`.

  Examples:
  ```json
  0
  ```

  ```json
  1
  ```

  ```json
  2
  ```

  ```json
  3
  ```

  ```json
  5
  ```

- <a id="properties/mongo_dsn"></a>**`mongo_dsn`** *(string, format: multi-host-uri, required)*: MongoDB connection string. Might include credentials. For more information see: https://naiveskill.com/mongodb-connection-string/. Length must be at least 1.

  Examples:
  ```json
  "mongodb://localhost:27017"
  ```

- <a id="properties/db_name"></a>**`db_name`** *(string, required)*: Name of the database located on the MongoDB server.

  Examples:
  ```json
  "my-database"
  ```

- <a id="properties/mongo_timeout"></a>**`mongo_timeout`**: Timeout in seconds for API calls to MongoDB. The timeout applies to all steps needed to complete the operation, including server selection, connection checkout, serialization, and server-side execution. When the timeout expires, PyMongo raises a timeout exception. If set to None, the operation will not time out (default MongoDB behavior). Default: `null`.
  - **Any of**
    - <a id="properties/mongo_timeout/anyOf/0"></a>*integer*: Exclusive minimum: `0`.
    - <a id="properties/mongo_timeout/anyOf/1"></a>*null*

  Examples:
  ```json
  300
  ```

  ```json
  600
  ```

  ```json
  null
  ```

- <a id="properties/log_level"></a>**`log_level`** *(string)*: The minimum log level to capture. Must be one of: "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", or "TRACE". Default: `"INFO"`.
- <a id="properties/log_format"></a>**`log_format`**: If set, will replace JSON formatting with the specified string format. If not set, has no effect. In addition to the standard attributes, the following can also be specified: timestamp, service, instance, level, correlation_id, and details. Default: `null`.
  - **Any of**
    - <a id="properties/log_format/anyOf/0"></a>*string*
    - <a id="properties/log_format/anyOf/1"></a>*null*

  Examples:
  ```json
  "%(timestamp)s - %(service)s - %(level)s - %(message)s"
  ```

  ```json
  "%(asctime)s - Severity: %(levelno)s - %(msg)s"
  ```

- <a id="properties/log_traceback"></a>**`log_traceback`** *(boolean)*: Whether to include exception tracebacks in log messages. Default: `true`.
- <a id="properties/processing_configuration_path"></a>**`processing_configuration_path`** *(string, format: path, required)*: Path to the config file used to populate the database with models, transformations and workflows.
- <a id="properties/processing_poll_pause"></a>**`processing_poll_pause`** *(integer)*: Seconds to sleep when no unprocessed AEMPacks are found. Default: `60`.
- <a id="properties/worker_id"></a>**`worker_id`** *(string, required)*: Unique identifier for a service instance used specifically for the reclamation logic.
- <a id="properties/config_lock_expiry_seconds"></a>**`config_lock_expiry_seconds`** *(integer)*: TTL in seconds for the config lock document. MongoDB automatically removes stale locks after this duration. Default: `120`.
- <a id="properties/config_lock_poll_interval"></a>**`config_lock_poll_interval`** *(integer)*: Seconds between polls when waiting for the config lock to be released. Default: `5`.
- <a id="properties/config_lock_timeout"></a>**`config_lock_timeout`** *(integer)*: Maximum seconds to wait for the config lock to be released before raising a TimeoutError. Default: `150`.

### Usage:

A template YAML file for configuring the service can be found at
[`./example_config.yaml`](./example_config.yaml).
Please adapt it, rename it to `.emts.yaml`, and place it in one of the following locations:
- in the current working directory where you execute the service (on Linux: `./.emts.yaml`)
- in your home directory (on Linux: `~/.emts.yaml`)

The config YAML file will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example_config.yaml`](./example_config.yaml)
can also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `emts_`,
e.g. for the `host` set an environment variable named `emts_host`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To use file secrets, please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.



## Architecture and Design:
<!-- Please provide an overview of the architecture and design of the code base.
Mention anything that deviates from the standard Triple Hexagonal Architecture and
the corresponding structure. -->

This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development

For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of VS Code
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as VS Code with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in VS Code and run the command
`Remote-Containers: Reopen in Container` from the VS Code "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant VS Code extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Inside the devcontainer, a command `dev_install` is available for convenience.
It installs the service with all development dependencies, and it installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./pyproject.toml`](./pyproject.toml) or the
[`lock/requirements-dev.txt`](./lock/requirements-dev.txt), run it again.

## License

This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## README Generation

This README file is auto-generated, please see [.readme_generation/README.md](./.readme_generation/README.md)
for details.
