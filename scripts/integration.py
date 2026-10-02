from contextlib import closing
import argparse
import json
import time
from pathlib import Path
import requests
from cassandra.cluster import Cluster
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from producer import events, publish, BASE_MS

URL = 'http://connect:8083'
RESULT = Path('results/observations.json')

def wait(label, fn, timeout=180):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            result = fn()
            if result:
                return result
        except (requests.RequestException, AssertionError) as e:
            last = str(e)
        time.sleep(1)
    raise AssertionError(f'Timed out: {label}; last={last}')

def api(method, path, **kw):
    r = requests.request(method, URL + path, timeout=30, **kw)
    r.raise_for_status()
    return r.json() if r.content else None

def healthy(name):
    state = api('GET', f'/connectors/{name}/status')
    if state['connector']['state'] == 'FAILED' or any(t['state'] == 'FAILED' for t in state['tasks']):
        raise RuntimeError(json.dumps(state))
    return state['connector']['state'] == 'RUNNING' and bool(state['tasks']) and all(t['state'] == 'RUNNING' for t in state['tasks'])

def register(name):
    config = json.loads(Path(f'configs/{name}.json').read_text())
    api('POST', '/connectors', json=config)
    wait(name, lambda: healthy(name))

def clean(session):
    for name in api('GET', '/connectors'):
        api('DELETE', f'/connectors/{name}')
    # Delete waits for task stop on this one-worker setup; verify absence before truncating.
    wait('connector deletion', lambda: not api('GET', '/connectors'))
    for table in ('rerate_edr_v1', 'rerate_edr_v2'):
        session.execute(f'TRUNCATE rerate.{table}')

def caught_up(admin, name, topic):
    healthy(name)
    with closing(KafkaConsumer(bootstrap_servers='kafka:9092')) as c:
        end = c.end_offsets([TopicPartition(topic, 0)])[TopicPartition(topic, 0)]
    offsets = admin.list_consumer_group_offsets('connect-' + name)
    committed = offsets.get(TopicPartition(topic, 0))
    return end > 0 and committed is not None and committed.offset >= end

def rows(session, table):
    return [dict(r._asdict()) for r in session.execute(f'SELECT * FROM rerate.{table}')]

def check(session, admin, names, counts, terminal_only):
    for name, topic in names:
        wait('committed offsets for ' + name, lambda: caught_up(admin, name, topic))
    def snapshot():
        v2, v1 = rows(session, 'rerate_edr_v2'), rows(session, 'rerate_edr_v1')
        assert (len(v2), len(v1)) == counts, (v2, v1)
        if terminal_only:
            assert {r['status'] for r in v1} <= {'SUCCESS', 'FAILED'}, v1
            assert len({r['ingestion_time'] for r in v1}) == len(v1)
            assert {r['job_id'] for r in v1} == {'JOB1', 'JOB2'}
            assert {r['txn_id'] for r in v1} == {'TXN1', 'TXN2'}
        if counts[0] == 10:
            expected = {(e['txnId'], e['eventSeqNo'], e['eventType'], e['operation'])
                        for _, e in list(events('JOB1','TXN1','SUCCESS',BASE_MS)) + list(events('JOB2','TXN2','FAILED',BASE_MS+60000))}
            assert {(r['txn_id'],r['event_seq_no'],r['event_type'],r['operation']) for r in v2} == expected
        if counts[0] == 2:
            assert {r['event_type'] for r in v2} == {'SUCCESS', 'FAILED'}
        return {'v2_count':len(v2),'v1_count':len(v1),'v2':v2,'v1':v1}
    return wait('persisted rows', snapshot)

def main(mode):
    admin = KafkaAdminClient(bootstrap_servers='kafka:9092')
    topics = ['rerate-edr'] if mode == 'experiments' else ['rerate-edr-terminal']
    for topic in topics:
        try: admin.create_topics([NewTopic(topic, 1, 1)])
        except TopicAlreadyExistsError: pass
    cluster = Cluster(['cassandra'])
    session = cluster.connect()
    for stmt in Path('schema/schema.cql').read_text().split(';'):
        if stmt.strip(): session.execute(stmt)
    plugins = api('GET', '/connector-plugins')
    datastax = [p for p in plugins if p['class'] == 'com.datastax.oss.kafka.sink.CassandraSinkConnector']
    assert len(datastax) == 1 and datastax[0]['version'] == '1.7.6', datastax
    Path('results/versions.json').write_text(json.dumps({'worker': api('GET', '/'), 'datastax': datastax}, indent=2) + '\n')
    evidence = json.loads(RESULT.read_text()) if RESULT.exists() else {}
    def save(): RESULT.write_text(json.dumps(evidence, indent=2, default=str) + '\n')
    try:
        if mode == 'experiments':
            clean(session)
            register('baseline')
            publish(events('JOB1','TXN1','SUCCESS',BASE_MS))
            evidence['baseline-first-transaction'] = check(session,admin,[('baseline','rerate-edr')],(5,5),False); save()
            publish(events('JOB2','TXN2','FAILED',BASE_MS+60000))
            evidence['baseline'] = check(session,admin,[('baseline','rerate-edr')],(10,10),False); save()
            for name, counts, terminals in [('unsupported-table-filter',(10,10),False), ('global-filter',(2,2),True), ('one-connector-routed',(10,2),True)]:
                clean(session)
                if name == 'unsupported-table-filter':
                    api('POST', '/connectors', json=json.loads(Path(f'configs/{name}.json').read_text()))
                    def rejected():
                        status = api('GET', f'/connectors/{name}/status')
                        failed = [t for t in status['tasks'] if t['state'] == 'FAILED']
                        return status if failed else None
                    status = wait('unsupported per-table filter rejection', rejected)
                    assert '.filter does not match' in json.dumps(status), status
                    v2, v1 = rows(session, 'rerate_edr_v2'), rows(session, 'rerate_edr_v1')
                    assert not v2 and not v1
                    evidence[name] = {'v2_count': 0, 'v1_count': 0, 'rejected': True, 'status': status}
                    save()
                    continue
                register(name)
                evidence[name] = check(session,admin,[(name,'rerate-edr')],counts,terminals)
                if name == 'one-connector-routed':
                    assert len(api('GET','/connectors')) == 1
                    assert api('GET', f'/connectors/{name}/config')['topics'] == 'rerate-edr'
                    assert 'rerate-edr-terminal-route' not in admin.list_topics()
                    evidence[name]['physical_topics'] = sorted(admin.list_topics())
                    # Restart task, then replay exact events: stable timestamps must prevent new history rows.
                    api('POST', f'/connectors/{name}/tasks/0/restart')
                    wait('routed restart', lambda: healthy(name))
                    publish(events('JOB1','TXN1','SUCCESS',BASE_MS))
                    publish(events('JOB2','TXN2','FAILED',BASE_MS+60000))
                    evidence['one-connector-replay'] = check(session,admin,[(name,'rerate-edr')],(10,2),True)
                save()
            clean(session)
        else:
            clean(session)
            register('fallback-v2'); register('fallback-v1')
            def projection_complete():
                offsets = admin.list_consumer_group_offsets('rerate-terminal-projection')
                v = offsets.get(TopicPartition('rerate-edr',0))
                with closing(KafkaConsumer(bootstrap_servers='kafka:9092')) as c:
                    end = c.end_offsets([TopicPartition('rerate-edr',0)])[TopicPartition('rerate-edr',0)]
                return v is not None and v.offset >= end
            wait('projection offset committed', projection_complete)
            evidence['fallback'] = check(session,admin,[('fallback-v2','rerate-edr'),('fallback-v1','rerate-edr-terminal')],(10,2),True); save()
        print(json.dumps({k:{x:v[x] for x in ('v2_count','v1_count')} for k,v in evidence.items()}, indent=2))
    finally:
        cluster.shutdown(); admin.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('mode', choices=['experiments','fallback'])
    main(parser.parse_args().mode)
