# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests with coverage
pytest --cov=ets --cov-report=xml tests

# Run a single test
pytest tests/test_aem_pack_registry/test_queue_and_claim.py::test_queue_creates_correct_document

# Lint and format
ruff check .
ruff format .
mypy .
```

Tests use `asyncio_mode = "strict"` — all async tests must be marked with `@pytest.mark.asyncio` or the module-level `pytestmark = pytest.mark.asyncio`.

## Architecture

ETS (Event Transformation Service) follows a hexagonal architecture via the [hexkit](https://github.com/ghga-de/hexkit) framework.

### Data Flow

1. Kafka → `EventSubTranslator` (inbound adapter) receives `AEMPack` events and calls `queue_unprocessed`
2. `AEMPackRegistry.queue_unprocessed` upserts the pack into MongoDB `unprocessed_aem_packs` with `correlation_id` from the active hexkit correlation context
3. `AEMPackRegistry.process_aem_packs` polls MongoDB in a loop, claims a pack, runs graph traversal, and publishes derived packs via the outbox DAO
4. Derived packs are published to Kafka through hexkit's `MongoKafkaDaoPublisherFactory` outbox pattern

### Processing Queue

`unprocessed_aem_packs` is accessed directly (bypassing the DAO) for atomic MongoDB operations. Key fields:

- `processor`: set to `service_instance_id` when claimed, `None` when done
- `processed_at`: timestamp when completed, `None` while in-flight or pending
- `needs_reprocessing`: `True` when a new version of the pack arrives while it is being processed — the current run completes and publishes, then the pack is picked up again

The polling loop checks in order: (1) own abandoned packs (`processor == service_instance_id, processed_at == None`) for crash recovery, (2) fresh unclaimed packs, (3) processed packs with `needs_reprocessing == True`.

### Graph Traversal

`PersistedConfig` holds `Model`, `Route`, and `Workflow` objects. On load, a topological order is computed once. `_traverse_graph` walks from the ingress pack through routes in topological order, applying `Workflow` transformations to produce all derived `AEMPack` outputs.

A "dirty map" (`model_name → aem_pack_id`) tracks previously derived packs for the same original ID. After traversal, any entries still in the dirty map represent unreachable packs that must be deleted.

### Key Layers

| Layer | Location |
|---|---|
| Core logic | `src/ets/core/aem_pack_registry.py` |
| Domain models | `src/ets/core/models.py` |
| Ports (interfaces) | `src/ets/ports/` |
| Adapters | `src/ets/adapters/inbound/` and `src/ets/adapters/outbound/` |
| DI wiring | `src/ets/inject.py` |
| Config | `src/ets/config.py` |

### MongoDB Update Patterns

`queue_unprocessed` uses an **aggregation pipeline update** (`update=[{$set: {...}}]`) rather than a plain update so that field references like `"$processor"` can be used in `$cond` expressions to conditionally preserve the current value of a field in the same document.

### Correlation IDs

`queue_unprocessed` calls `get_correlation_id()` internally — callers must have a correlation context active (via `async with set_correlation_id(...):`). Tests that call `queue_unprocessed` directly must set up this context explicitly.
