"""Canonical V2 producer only: never publishes a V1 record or compatibility topic."""
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
BASE_MS = 1790928000000  # 2026-10-02T08:00:00Z

def events(job, txn, terminal, start_ms):
    kinds = [('SCHEDULED', None), ('ATTEMPT', None),
             ('OPERATION', 'KAFKA_COMMIT_INTENT'),
             ('OPERATION', 'KAFKA_COMMIT_COMPLETED'), (terminal, None)]
    for seq, (kind, operation) in enumerate(kinds):
        stamp = start_ms + seq * 1000
        yield stamp, dict(subscriberId='SUB001', jobId=job, txnId=txn,
                         eventSeqNo=seq, eventType=kind, operation=operation,
                         eventTime=datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat(),
                         status=kind if seq == 4 else 'IN_PROGRESS')

def publish(batch):
    producer = KafkaProducer(bootstrap_servers='kafka:9092', acks='all',
                             value_serializer=lambda x: json.dumps(x).encode())
    try:
        for stamp, event in batch:
            producer.send('rerate-edr', key=b'SUB001', value=event, timestamp_ms=stamp).get(timeout=30)
        producer.flush()
    finally:
        producer.close()

if __name__ == '__main__':
    publish(events('JOB1', 'TXN1', 'SUCCESS', BASE_MS))
    publish(events('JOB2', 'TXN2', 'FAILED', BASE_MS + 60000))
