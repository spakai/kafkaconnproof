# Rerating EDR compatibility projection PoC

Rerating publishes **one canonical V2 stream**, `rerate-edr`. It has no knowledge of V1. V1 is a downstream, temporary compatibility projection.

This repository tests DataStax Kafka Sink Connector **1.7.6**, the latest GitHub release verified on 2026-10-02. Kafka and Connect use Confluent Platform 7.8.0 (Kafka 3.8); Cassandra is 4.1.8. Version pins keep the experiment repeatable; “latest” is not a moving Docker tag for the connector.

## Run

Prerequisites: Docker Engine / Docker Desktop with Compose v2+, internet access for the first build, and approximately 6 GB available Docker memory. Under WSL, enable Docker Desktop integration. Host Java/Python are unnecessary.

```bash
./scripts/test.sh
```

The script resets **this Compose project's** containers and volumes, builds the Java predicate/projection and test runner, starts Kafka/Cassandra/Connect, creates schema and topics, runs all experiments, and leaves the fallback running for inspection. Do not reuse the `rerate-poc` project for unrelated services. A successful run exits zero; a failure exits nonzero and retains logs.

Artifacts:

- `results/observations.json`: actual Cassandra rows, counts and physical topics.
- `results/report.md`: concise result generated only after acceptance succeeds.
- `results/containers.log`: infrastructure/connector/projection logs.
- `configs/*.json`: complete Kafka Connect REST registration bodies.
- `schema/schema.cql`: exact V1/V2 schemas.
- `scripts/producer.py`: canonical producer with SUCCESS and FAILED lifecycle fixtures.
- `java/src/main/java/poc/Projection.java`: small Java consumer/producer fallback.

```bash
# Inspect the running fallback
curl -s localhost:8083/connectors
curl -s localhost:8083/connectors/fallback-v1/status
docker compose exec cassandra cqlsh -e 'SELECT * FROM rerate.rerate_edr_v1;'
docker compose exec cassandra cqlsh -e 'SELECT * FROM rerate.rerate_edr_v2;'
# Stop and discard the PoC data
docker compose --profile fallback --profile test down -v
# Run only Java behavior tests (requires Java 17+ and Maven)
mvn -f java/pom.xml test
```

## Experiments

Each experiment uses one source topic `rerate-edr`. Connectors are stopped and tables truncated between cases; new connector names consume the same retained source events from the beginning. Fixtures initially contain five JOB1/TXN1 events ending in SUCCESS, then five JOB2/TXN2 events ending in FAILED, all for SUB001. There is no RETRY event. The first baseline is checked at five events, and both transactions are checked at ten events. Final assertions compare V2 event identities/types/operations as well as counts.

| Config | Question | Expected V2 / V1 distinct rows |
|---|---|---|
| `baseline.json` | Does ordinary mapping send each record to both tables? | 10 / 10 |
| `unsupported-table-filter.json` | Does a proposed table `.filter` property do anything? | Rejected; task FAILED, no rows |
| `global-filter.json` | Does Filter remove nonterminal records before both mappings? | 2 / 2 |
| `one-connector-routed.json` | Can conditional routing select different mapping sets? | 10 / 2 |
| `fallback-v2.json`, `fallback-v1.json` | Does the requested terminal-topic projection work? | 10 / 2 |

These are test expectations, not evidence by themselves. Consult the recorded observations/report for the executed result. `.filter` is deliberately an unsupported probe, **not a documented DataStax configuration option**. One guessed option alone does not prove absence of a feature; the conclusion also uses the version-pinned mapping implementation/config definitions cited below.

The suite waits for source offsets to be committed and checks task health before querying Cassandra. It verifies V1 contains only SUCCESS/FAILED, retains both JOB1 and JOB2, has two distinct historical keys, and V2 retains all ten lifecycle records. The one-connector routing case restarts its task and republishes identical events to test stable keys under replay. The fallback consumes those replayed events too; its at-least-once duplicates upsert existing rows.

## Native filters versus one-connector routing

The DataStax table configuration has no event predicate/filter option. Kafka Connect applies SMTs before invoking the sink; returning null from Filter removes the whole record. Consequently a terminal-only Filter ahead of two table mappings loses the nonterminal V2 events too.

Apache's stock predicates match topic names, headers, or tombstones, not arbitrary `value.eventType` content. `poc.TerminalPredicate` is a small custom **predicate**, not a custom connector or a producer change. The global-filter experiment uses that predicate with the stock Filter SMT.

The routing experiment uses the same custom predicate with the stock RegexRouter SMT:

```text
Kafka source topic: rerate-edr (the only subscribed topic)
  nonterminal -> unchanged record topic -> V2 mapping
  terminal    -> in-memory topic rerate-edr-terminal-route -> V2 and V1 mappings
```

`rerate-edr-terminal-route` is a logical mapping name, not a physical Kafka topic. RegexRouter changes `SinkRecord.topic()` inside Connect; it neither republishes nor duplicates records. A terminal record fans out through the two mappings associated with that logical topic; a nonterminal record sees only the V2 mapping. The test asserts there is exactly one connector, its configured source is `rerate-edr`, and no physical route topic exists.

A custom SMT could make the same topic-name decision directly. A custom SMT that merely returns null has the same global-drop limitation as Filter. The Transform API returns one record (or null), not independently filtered per-table copies. Changing the mapping selection is the relevant escape hatch.

Thus **“no native per-table filtering” does not imply “one connector is impossible with custom code.”** The experimental result determines whether this version supports the routing workaround.

## Fallback and production choice

```text
rerate-edr -> DataStax V2 Sink -----------------> rerate_edr_v2
          -> Java terminal projection
               -> rerate-edr-terminal
                    -> DataStax V1 Sink -------> rerate_edr_v1
```

The projection emits only SUCCESS/FAILED and only the V1 fields. It waits for each Kafka produce acknowledgment before committing source offsets, with producer idempotence enabled. Delivery across process restarts remains **at least once**, not transactional exactly once. The two sinks converge independently; this is not a cross-table atomic write. Rerating still emits only canonical V2 events.

For production, prefer the fallback if operational independence, replay of the compatibility stream, and removal of V1 without touching V2 are priorities. The custom-predicate routing case is an option if one connector is a hard constraint, but should also undergo failure/rebalance/load testing, particularly because connector failure-offset handling refers to the transformed topic. This PoC tests healthy delivery, restart and replay, not a complete production certification.

### Historical V1 keys and timestamp semantics

V1 keeps the requested `PRIMARY KEY ((subscriber_id), ingestion_time)` and a reduced field set. Fixtures deliberately use distinct, deterministic Kafka record timestamps, one second apart with a one-minute gap between jobs. The one-connector cases use InsertField to copy the Kafka record timestamp to `ingestionTime` and TimestampConverter to format it as ISO text; the projection copies the original source timestamp into its output. This gives the same key on replay without putting compatibility fields into the canonical event value.

**For this PoC, `ingestion_time` means the source Kafka record timestamp**, not the Cassandra write clock or a newly sampled projection clock. Mapping `now()` on each retry would create spurious history. Cassandra timestamps have millisecond precision: two terminal events for the same subscriber with the same timestamp overwrite under this exact schema. The fixture demonstrates history for distinct timestamps; it does not solve arbitrary timestamp collisions. Production must settle the timestamp contract/unique-key allocation in the compatibility layer, or explicitly approve a tie-breaker in V1's key. No upstream producer redesign is implemented here.

## Primary sources

- [DataStax 1.7.6 release](https://github.com/datastax/kafka-sink/releases/tag/1.7.6).
- [DataStax documented multiple-table mapping](https://docs.datastax.com/en/kafka/doc/kafka/kafkaMapMultipleTables.html).
- [1.7.6 Kafka record adapter](https://github.com/datastax/kafka-sink/blob/1.7.6/sink/src/main/java/com/datastax/oss/kafka/sink/KafkaSinkRecordAdapter.java): exposes the transformed `record.topic()`.
- [Commons 1.0.16 table settings](https://github.com/datastax/messaging-connectors-commons/blob/1.0.16/common/src/main/java/com/datastax/oss/common/sink/config/TableConfig.java): mapping/query/TTL/consistency settings, no table predicate.
- [Commons 1.0.16 mapping loop](https://github.com/datastax/messaging-connectors-commons/blob/1.0.16/common/src/main/java/com/datastax/oss/common/sink/AbstractSinkTask.java): `mapAndQueueRecord` selects topic configuration, then iterates its tables.
- [Kafka 3.8 WorkerSinkTask](https://github.com/apache/kafka/blob/3.8.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerSinkTask.java): applies transformations and drops null before `task.put`.
- [Kafka 3.8 RegexRouter](https://github.com/apache/kafka/blob/3.8.0/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/RegexRouter.java), [Filter](https://github.com/apache/kafka/blob/3.8.0/connect/transforms/src/main/java/org/apache/kafka/connect/transforms/Filter.java), [Transform API](https://github.com/apache/kafka/blob/3.8.0/connect/api/src/main/java/org/apache/kafka/connect/transforms/Transformation.java).
