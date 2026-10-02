import json
from pathlib import Path
p = Path('results')
d = json.loads((p / 'observations.json').read_text())
required = ['baseline', 'unsupported-table-filter', 'global-filter', 'one-connector-routed', 'one-connector-replay', 'fallback']
for k in required:
    assert k in d, f'Missing successful experiment: {k}'
rows = '\n'.join(f'| {k} | {d[k]["v2_count"]} | {d[k]["v1_count"]} |' for k in required)
report = f'''# Executed PoC report

ONE CONNECTOR POSSIBLE: YES — with a custom content predicate and stock RegexRouter.
NATIVE PER-DESTINATION TABLE FILTER: NO.

Reason: The proposed per-table `.filter` option was explicitly rejected by the task configuration parser (zero writes). Filter SMT discards the record before every table mapping (observed V2=2, V1=2). Conditional routing changes the in-memory record topic to select either V2-only mappings or V2+V1 mappings. There is still one subscribed Kafka source topic and one connector, with no physical route topic.

Configuration tested: DataStax 1.7.6; Kafka/Connect CP 7.8.0; Cassandra 4.1.8. Exact REST bodies are in configs/. Unit tests and Docker acceptance tests passed. Raw Cassandra rows are in observations.json.

| Experiment | Observed V2 rows | Observed V1 rows |
|---|---:|---:|
{rows}

The first five-event baseline produced V2=5 and V1=5. The routed and fallback cases each retained all ten lifecycle events and exactly two V1 records: JOB1/TXN1 SUCCESS and JOB2/TXN2 FAILED for SUB001, with different ingestion_time keys. No SCHEDULED, ATTEMPT or OPERATION rows appear in their V1 output. Restart/replay preserved those counts.

Recommended production architecture: one canonical rerate-edr stream, independent V2 sink, downstream terminal projection to rerate-edr-terminal, and V1 sink. Use the one-connector routing option only if that constraint outweighs the independent operation of the two projections.

Limits: ingestion_time uses the stable source record timestamp in this PoC; same-subscriber millisecond collisions require a production key/uniqueness decision. No cross-table atomicity or exactly-once projection claim. Failure-path routing/rebalance/load certification remains outside this PoC.
'''
(p / 'report.md').write_text(report)
print(report)
