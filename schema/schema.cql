CREATE KEYSPACE IF NOT EXISTS rerate WITH replication = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS rerate.rerate_edr_v2 (
 subscriber_id text, event_time timestamp, txn_id text, event_seq_no int,
 job_id text, event_type text, operation text, status text,
 PRIMARY KEY ((subscriber_id), event_time, txn_id, event_seq_no)
);
CREATE TABLE IF NOT EXISTS rerate.rerate_edr_v1 (
 subscriber_id text, ingestion_time timestamp, job_id text, txn_id text, status text,
 PRIMARY KEY ((subscriber_id), ingestion_time)
);
