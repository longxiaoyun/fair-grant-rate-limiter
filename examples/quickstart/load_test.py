#!/usr/bin/env python3
"""Local load and fault probes. Isolated Redis only; no production endpoints."""
import argparse
import csv
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import subprocess
import time
import run as demo

HERE = Path(__file__).resolve().parent


def redis_command(port, *args, timeout=2):
    with socket.create_connection(('127.0.0.1', port), timeout) as sock:
        sock.settimeout(timeout)
        data = b'*%d\r\n' % len(args)
        for arg in args:
            value = str(arg).encode()
            data += b'$%d\r\n' % len(value) + value + b'\r\n'
        sock.sendall(data)
        stream = sock.makefile('rb')
        def read():
            line = stream.readline()
            if not line:
                raise OSError('Redis closed connection')
            tag, value = line[:1], line[1:-2]
            if tag == b'-':
                raise OSError(value.decode())
            if tag == b':':
                return int(value)
            if tag == b'$':
                count = int(value)
                if count < 0:
                    return None
                content = stream.read(count)
                stream.read(2)
                return content.decode()
            if tag == b'*':
                return [read() for _ in range(int(value))]
            return value.decode()
        return read()


def info(port):
    raw = redis_command(port, 'INFO', 'ALL')
    return dict(line.split(':', 1) for line in raw.splitlines() if line and not line.startswith('#') and ':' in line)


def command_count(data, name):
    fields = dict(x.split('=') for x in data.get('cmdstat_' + name, 'calls=0').split(','))
    return int(fields['calls'])


def percentile(weighted, q):
    if not weighted:
        return None
    weighted.sort(key=lambda x: x[0])
    target = sum(weight for _, weight in weighted) * q
    cumulative = 0
    for value, weight in weighted:
        cumulative += weight
        if cumulative >= target:
            return round(value / 1e6, 4)
    return round(weighted[-1][0] / 1e6, 4)


def trial(name, config, cp, java, out, seconds, warmup=3, faults=False):
    folder = out / (name + '-' + str(time.time_ns()))
    folder.mkdir(parents=True)
    port = demo.free_port()
    redis = None
    workers, logs, samples, events = [], [], [], []
    killed = set()
    paused = False
    redis_log = (folder / 'redis.log').open('w')
    def start_redis():
        proc = subprocess.Popen([shutil.which('redis-server'), '--bind', '127.0.0.1', '--port', str(port),
            '--save', '', '--appendonly', 'yes' if faults else 'no', '--appendfsync', 'everysec',
            '--dir', str(folder)], stdout=redis_log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while True:
            try:
                if redis_command(port, 'PING') == 'PONG':
                    return proc
            except OSError:
                if proc.poll() is not None or time.monotonic() > deadline:
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill(); proc.wait()
                    raise RuntimeError('Redis startup failed: ' + str(folder))
                time.sleep(.02)
    try:
        redis = start_redis()
        baseline = info(port)
        for n in range(config['jvms']):
            log = (folder / ('node-%02d.log' % n)).open('w')
            logs.append(log)
            args = [java, '-Xms16m', '-Xmx128m', '-XX:+UseSerialGC', '-cp', cp, 'example.LoadWorker',
                str(port), str(folder), 'node-%02d' % n, str(config['threads']), str(config['resources']),
                str(warmup), str(seconds), str(config['window']).lower(), str(config['respect']).lower(),
                str(config['ping']).lower(), str(faults).lower(), str(config['rate']), str(config['burst']),
                str(config['window_ms']), str(config['cap']), str(config['state_idle_ms'])]
            workers.append(subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 30
        while len(list(folder.glob('*.ready'))) != config['jvms']:
            if any(p.poll() is not None for p in workers) or time.monotonic() > deadline:
                raise RuntimeError('Load worker startup failed: ' + str(folder))
            time.sleep(.05)
        start = time.monotonic()
        (folder / 'start').touch()
        # Fault timings are fractions of the measured interval; keep enough recovery time after each.
        schedule = [(seconds*.20, 'kill_client'), (seconds*.35, 'disconnect'),
                    (seconds*.48, 'script_flush'), (seconds*.60, 'freeze'),
                    (seconds*.78, 'restart')] if faults else []
        applied = set()
        while any(p.poll() is None for p in workers):
            elapsed = time.monotonic() - start - warmup
            if elapsed > seconds + 15:
                raise RuntimeError('Load worker deadline: ' + str(folder))
            if faults:
                for at, action in schedule:
                    if elapsed < at or action in applied:
                        continue
                    applied.add(action)
                    event = dict(action=action, start_us=time.time_ns()//1000, elapsed=elapsed)
                    if action == 'kill_client':
                        workers[0].kill(); workers[0].wait(); killed.add(0)
                    elif action == 'disconnect':
                        redis_command(port, 'CLIENT', 'KILL', 'TYPE', 'normal', 'SKIPME', 'yes')
                    elif action == 'script_flush':
                        redis_command(port, 'SCRIPT', 'FLUSH')
                    elif action == 'freeze':
                        os.kill(redis.pid, signal.SIGSTOP); paused = True
                        time.sleep(3)
                        os.kill(redis.pid, signal.SIGCONT); paused = False
                    elif action == 'restart':
                        redis.terminate(); redis.wait(timeout=10)
                        time.sleep(2)
                        redis = start_redis()  # Same AOF and directory; this is a clean restart, not failover.
                    event['end_us'] = time.time_ns()//1000
                    events.append(event)
                    print('FAULT', action, flush=True)
            sample = dict(elapsed=round(time.monotonic() - start - warmup, 3), at_us=time.time_ns()//1000)
            try:
                data = info(port)
                sample['redis'] = {key: data.get(key) for key in [
                    'used_memory', 'used_memory_rss', 'connected_clients', 'total_commands_processed',
                    'used_cpu_sys', 'used_cpu_user', 'aof_current_size', 'expired_keys']}
                sample['redis']['eval_calls'] = command_count(data, 'eval') + command_count(data, 'evalsha')
                sample['redis']['ping_calls'] = command_count(data, 'ping')
            except OSError as error:
                sample['redis_error'] = str(error)
            sample['jvms'] = {}
            for f in folder.glob('*.snapshot.json'):
                if any(f.name == 'node-%02d.snapshot.json' % n for n in killed):
                    continue
                try:
                    sample['jvms'][f.name] = json.loads(f.read_text())
                except (OSError, ValueError):
                    pass
            samples.append(sample)
            time.sleep(1)
        for n, p in enumerate(workers):
            if n not in killed and p.returncode != 0:
                raise RuntimeError('Load worker failed: ' + str(folder))
        completed = []
        for n in range(config['jvms']):
            path = folder / ('node-%02d.result.json' % n)
            if n in killed:
                path = folder / ('node-%02d.snapshot.json' % n)
            completed.append(json.loads(path.read_text()))
        thread_stats = [s for process in completed for s in process['threads']]
        totals = {k: sum(s[k] for s in thread_stats) for k in ['calls', 'grants', 'waits', 'errors', 'degraded', 'down']}
        assert totals['errors'] == 0 and totals['degraded'] == 0, totals
        if not faults:
            assert totals['down'] == 0, totals
        weighted = []
        for s in thread_stats:
            values = s.get('latency_sample_ns', [])
            if values:
                weighted.extend((v, s['calls']/len(values)) for v in values)
        last = info(port)
        time.sleep(7 if config['state_idle_ms'] < 5000 else 5)  # Short-retention case also waits for 5s leases.
        cool = info(port)
        keys = redis_command(port, 'DBSIZE')
        if config['state_idle_ms'] < 5000:
            assert keys == 0, 'Idle resource state did not expire'
        measured = [s for s in samples if 0 <= s['elapsed'] <= seconds and s.get('redis')]
        latency_mean = sum(s['latency_sum_ns'] for s in thread_stats)/max(1,totals['calls'])/1e6
        report = dict(name=name, config=config, seconds=seconds, warmup_seconds=warmup, totals=totals,
            calls_per_second=round(totals['calls']/seconds,2), grants_per_second=round(totals['grants']/seconds,2),
            latency_ms=dict(mean=round(latency_mean,4), p50_sample=percentile(weighted,.5),
                p95_sample=percentile(weighted,.95), p99_sample=percentile(weighted,.99),
                max=round(max(s['latency_max_ns'] for s in thread_stats)/1e6,4)),
            redis_memory_bytes=dict(baseline=int(baseline['used_memory']), end=int(last['used_memory']),
                after_receipt_expiry=int(cool['used_memory']), retained_keys=keys,
                peak=max(int(s['redis']['used_memory']) for s in measured)),
            max_combined_jvm_heap_bytes=max(sum(j['heap_used'] for j in s['jvms'].values()) for s in samples),
            gc_collection_ms=sum(j['gc_ms'] for j in completed),
            client_cpu_seconds=sum(j['cpu_ns'] for j in completed if j['cpu_ns']>=0)/1e9,
            fault_events=events, killed_worker_counters='last snapshot; up to 1s missing' if killed else None)
        if not faults and len(measured)>1:
            first,last_sample=measured[0],measured[-1]
            span=last_sample['elapsed']-first['elapsed']
            report['redis_sampled_rates'] = {
                'script_rpc_per_second':round((last_sample['redis']['eval_calls']-first['redis']['eval_calls'])/span,2),
                'ping_rpc_per_second':round((last_sample['redis']['ping_calls']-first['redis']['ping_calls'])/span,2),
                'internal_and_external_commands_per_second':round((int(last_sample['redis']['total_commands_processed'])-int(first['redis']['total_commands_processed']))/span,2),
                'cpu_core_fraction':round(sum(float(last_sample['redis'][k])-float(first['redis'][k]) for k in ['used_cpu_sys','used_cpu_user'])/span,3)}
        if faults:
            grants=[]
            for f in folder.glob('*-grants.csv'):
                for row in csv.reader(f.open()):
                    if row:
                        grants.append(dict(before=int(row[0]),after=int(row[1]),client=row[2]))
            grants.sort(key=lambda g:g['after'])
            assert len(grants)>config['cap'], 'Too few grants to exercise rolling windows'
            uncertain=0
            for i in range(config['cap'],len(grants)):
                group=grants[i-config['cap']:i+1]
                upper=max(g['after'] for g in group)-min(g['before'] for g in group)
                lower=max(g['before'] for g in group)-min(g['after'] for g in group)
                assert upper>=config['window_ms']*1000, 'Definite grant-window violation'
                uncertain += lower<config['window_ms']*1000
            for event in events:
                if event['action'] in ('freeze','restart'):
                    # Exclude calls/responses already in flight at the transition.
                    inner_start=event['start_us']+500000
                    inner_end=event['end_us']-100000
                    assert not any(g['before']>=inner_start and g['after']<=inner_end for g in grants), 'Grant while Redis unavailable'
                recovered=[g for g in grants if g['before']>event['end_us']]
                assert recovered, 'No progress after fault'
                event['first_observed_grant_ms']=(recovered[0]['after']-event['end_us'])/1000
                assert event['first_observed_grant_ms']<7000, 'Recovery exceeded lease + allowance'
                if event['action']=='kill_client':
                    assert not any(g['client'].startswith('node-00-') and g['before']>event['end_us'] for g in grants)
            counts={}
            for g in grants:
                counts[g['client']]=counts.get(g['client'],0)+1
            alive=[counts.get('node-%02d-0'%n,0) for n in range(1,config['jvms'])]
            assert min(alive)>0, 'Starved surviving client'
            # Every surviving node must acquire again AFTER the final restart, not merely earlier.
            last_fault=events[-1]['end_us']
            tail={g['client'] for g in grants if g['before']>last_fault}
            assert all('node-%02d-0'%n in tail for n in range(1,config['jvms'])), 'Some clients failed to recover'
            report['soak'] = dict(observed_grants=len(grants), surviving_client_grants_min=min(alive),
                surviving_client_grants_max=max(alive), uncertain_window_groups=uncertain,
                definite_window_violations=0, grants_during_outage_interior=0,
                max_observed_grant_gap_ms=max(b['after']-a['after'] for a,b in zip(grants,grants[1:]))/1000,
                per_client=counts)
        (folder/'report.json').write_text(json.dumps(report,indent=2))
        (folder/'samples.json').write_text(json.dumps(samples,indent=2))
        print(f"DONE {name}: calls/s={report['calls_per_second']}, grants/s={report['grants_per_second']}, "
              f"p95={report['latency_ms']['p95_sample']}ms, errors={totals['errors']}, down={totals['down']}",flush=True)
        return report
    finally:
        for p in workers:
            if p.poll() is None:
                p.terminate()
        for p in workers:
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        for f in logs:f.close()
        if redis is not None and redis.poll() is None:
            if paused:os.kill(redis.pid,signal.SIGCONT)
            redis.terminate()
            try:redis.wait(timeout=10)
            except subprocess.TimeoutExpired:redis.kill();redis.wait()
        redis_log.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['perf','soak','all'],default='all')
    parser.add_argument('--case', help='Optional single performance case, e.g. hot-30-cooperative')
    parser.add_argument('--seconds',type=int,default=10,help='Measured seconds per performance case')
    parser.add_argument('--no-ping',action='store_true',help='Disable testOnBorrow in the soak comparison')
    parser.add_argument('--soak-seconds',type=int,default=180)
    parser.add_argument('--repeats',type=int,default=2)
    args=parser.parse_args()
    if args.seconds<5 or args.soak_seconds<90 or args.repeats<1:parser.error('seconds >=5, soak-seconds >=90, repeats >=1')
    for tool in ['redis-server','java']:
        if not shutil.which(tool):parser.error('Missing '+tool)
    if not (HERE/'target/classpath.txt').exists() or not (HERE/'target/classes/example/LoadWorker.class').exists():
        parser.error('Build first: mvn -f examples/quickstart/pom.xml compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt')
    cp=str(HERE/'target/classes')+os.pathsep+(HERE/'target/classpath.txt').read_text().strip()
    java=str(Path(os.environ['JAVA_HOME'])/'bin/java') if os.environ.get('JAVA_HOME') else shutil.which('java')
    out=HERE/'target'/('load-'+str(time.time_ns()));out.mkdir(parents=True)
    base=dict(jvms=1,threads=1,resources=1,window=False,respect=False,ping=True,
              rate=1000000,burst=1000000,window_ms=1000,cap=1000000,state_idle_ms=60000)
    cases=[('single',{}),('single-window',dict(window=True)),
           ('hot-30',dict(threads=30,window=True)),
           ('hot-30-cooperative',dict(threads=30,window=True,respect=True)),
           ('resources-100',dict(threads=30,resources=100,window=True)),
           ('resources-100-no-ping',dict(threads=30,resources=100,window=True,ping=False)),
           ('resources-5000',dict(resources=5000,window=True)),
           ('resources-5000-retention',dict(resources=5000,window=True,state_idle_ms=1000,ping=False)),
           ('hot-30-default',dict(threads=30,window=True,respect=True,ping=False))]
    if args.case:
        cases=[case for case in cases if case[0]==args.case]
        if not cases:parser.error('Unknown performance case')
    reports=[]
    if args.mode in ('perf','all'):
        for repeat in range(args.repeats):
            # Reverse the second pass to reduce fixed-order bias.
            for name,overrides in (cases if repeat%2==0 else list(reversed(cases))):
                reports.append(trial(name+'-r'+str(repeat+1),dict(base,**overrides),cp,java,out,args.seconds))
    if args.mode in ('soak','all'):
        soak=dict(base,jvms=30,threads=1,window=True,respect=True,ping=not args.no_ping,rate=5,burst=1,window_ms=15000,cap=75)
        reports.append(trial('soak-30-jvms'+('-no-ping' if args.no_ping else ''),soak,cp,java,out,args.soak_seconds,faults=True))
    env=dict(platform=platform.platform(),python=platform.python_version(),logical_cpus=os.cpu_count(),
             java=subprocess.run([java,'-version'],capture_output=True,text=True).stderr,
             redis=subprocess.check_output(['redis-server','--version'],text=True),
             persistence='Performance: disabled; soak: AOF everysec, clean restart preserves state',
             caveats='Local loopback; shared host; closed-loop load; sampled percentiles; not a production SLA')
    result=dict(environment=env,reports=reports)
    (out/'summary.json').write_text(json.dumps(result,indent=2))
    (HERE/'target/latest-load-report.json').write_text(json.dumps(result,indent=2))
    print('Reports:',out,flush=True)


if __name__=='__main__':main()
