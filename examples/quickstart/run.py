#!/usr/bin/env python3
"""Build a standalone consumer, start isolated Redis/HTTP services, run real JVMs."""
import argparse
import collections
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SCENARIOS = {
    'quick': dict(nodes=3, batches=4, rate=.6, burst=6, window=10000, cap=6, resources=['tableA']),
    'idle': dict(nodes=3, idle_nodes=1, batches=3, rate=.6, burst=6, window=10000, cap=6, resources=['tableA']),
    'fleet': dict(nodes=30, batches=3, rate=4.8, burst=1, window=15000, cap=75, resources=['tableA', 'tableB']),
    'window': dict(nodes=3, batches=26, rate=5, burst=5, window=15000, cap=75, resources=['tableA']),
    'retry-paced': dict(nodes=3, batches=2, rate=2.5, burst=1, window=1000, cap=3, resources=['tableA', 'tableB']),
    'retry': dict(nodes=3, batches=2, rate=4, burst=2, window=1000, cap=3, resources=['tableA', 'tableB']),
}

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def run_scenario(name, cp, java):
    cfg = SCENARIOS[name]
    folder = HERE / 'target' / ('run-' + name + '-' + str(time.time_ns()))
    folder.mkdir(parents=True)
    events, ready, workers, handles = [], set(), [], []
    lock = threading.Lock()
    start = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            self.send_response(204 if start.is_set() else 503)
            self.end_headers()

        def do_POST(self):
            parsed = urllib.parse.urlparse(self.path)
            q = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
            if parsed.path == '/ready':
                with lock:
                    ready.add(q['node'])
                status = 204
            elif parsed.path == '/commit':
                # One known rejection, before business success. The next attempt must get a new token.
                status = 503 if name.startswith('retry') and q['operation'] == 'node-00-tableA-0' and q['attempt'] == '0' else 204
                with lock:
                    events.append(dict(q, status=status, received_us=time.time_ns() // 1000))
            else:
                status = 404
            self.send_response(status)
            self.end_headers()

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = free_port()
    redis_log = open(folder / 'redis.log', 'w')
    redis = None
    try:
        redis = subprocess.Popen([shutil.which('redis-server'), '--bind', '127.0.0.1', '--port', str(port),
                                  '--save', '', '--appendonly', 'no', '--dir', str(folder)],
                                 stdout=redis_log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    break
            except OSError:
                if redis.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('Redis startup failed; see ' + str(folder))
                time.sleep(.02)
        for n in range(cfg['nodes']):
            log = open(folder / ('node-%02d.log' % n), 'w')
            handles.append(log)
            workers.append(subprocess.Popen([java, '-Xms16m', '-Xmx64m', '-XX:+UseSerialGC', '-cp', cp, 'example.DemoWorker',
                str(port), 'http://127.0.0.1:' + str(server.server_port), 'node-%02d' % n,
                str(0 if n >= cfg['nodes'] - cfg.get('idle_nodes', 0) else cfg['batches']), str(cfg['rate']), str(cfg['burst']), str(cfg['window']), str(cfg['cap']),
                ','.join(cfg['resources'])], stdout=log, stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 30
        while True:
            with lock:
                count = len(ready)
            if count == cfg['nodes']:
                break
            if any(p.poll() is not None for p in workers) or time.monotonic() > deadline:
                raise RuntimeError('Workers failed to become ready; see ' + str(folder))
            time.sleep(.02)
        started = time.monotonic()
        start.set()
        while any(p.poll() is None for p in workers):
            if time.monotonic() - started > 95:
                raise RuntimeError('Scenario deadline; see ' + str(folder))
            time.sleep(.1)
        if any(p.returncode != 0 for p in workers):
            raise RuntimeError('Worker failed; see ' + str(folder))
        for f in handles:
            f.close()
        grants = []
        for log in folder.glob('node-*.log'):
            for line in log.read_text().splitlines():
                if line.startswith('GRANT,'):
                    _, resource, op, attempt, before, after = line.split(',')
                    grants.append(dict(resource=resource, operation=op, attempt=attempt,
                                       before=int(before), after=int(after)))
        active_nodes = cfg['nodes'] - cfg.get('idle_nodes', 0)
        expected = active_nodes * cfg['batches'] * len(cfg['resources'])
        successes = [e for e in events if e['status'] == 204]
        assert len(successes) == expected, 'Missing business operations'
        assert len({e['operation'] for e in successes}) == expected, 'Duplicated business operation'
        assert len(events) == expected + (name.startswith('retry')), 'Unexpected HTTP attempt count'
        assert collections.Counter((g['operation'], g['attempt']) for g in grants) == collections.Counter(
            (e['operation'], e['attempt']) for e in events), 'Each HTTP attempt needs its own acquisition'
        if name == 'idle':
            assert time.monotonic() - started < 5, 'Idle node blocked ready clients'
        results = {}
        for resource in cfg['resources']:
            gs = sorted([g for g in grants if g['resource'] == resource], key=lambda g: g['after'])
            # Public API does not return grant time. Each grant lies between the call start and callback start.
            # Report uncertain boundary cases explicitly; do not silently add a tolerance to the quota.
            uncertain = 0
            for i in range(cfg['cap'], len(gs)):
                group = gs[i-cfg['cap']:i+1]
                upper_span = max(g['after'] for g in group) - min(g['before'] for g in group)
                lower_span = max(g['before'] for g in group) - min(g['after'] for g in group)
                assert upper_span >= cfg['window'] * 1000, 'Definite window violation'
                uncertain += lower_span < cfg['window'] * 1000
            counts = collections.Counter(e['node'] for e in successes if e['resource'] == resource)
            assert len(counts) == active_nodes and set(counts.values()) == {cfg['batches']}, 'Starved node'
            for n in range(active_nodes, cfg['nodes']):
                assert counts.get('node-%02d' % n, 0) == 0, 'Idle node executed work'
            arrivals = sorted(e['received_us'] for e in events if e['resource'] == resource)
            left, peak = 0, 0
            for right, at in enumerate(arrivals):
                while arrivals[left] <= at - cfg['window'] * 1000:
                    left += 1
                peak = max(peak, right-left+1)
            results[resource] = dict(max_http_calls_in_window=peak, grants=len(gs), successes=sum(counts.values()), per_node=dict(counts),
                uncertain_boundary_groups=uncertain,
                max_acquisition_latency_ms=max(g['after']-g['before'] for g in gs)/1000)
        http_within_limit = all(v['max_http_calls_in_window'] <= cfg['cap'] for v in results.values())
        if name in ('fleet', 'retry-paced'):
            assert http_within_limit, 'Paced HTTP scenario exceeded observed downstream limit'
        report = dict(observed_http_window_pass=http_within_limit, scenario=name, config=cfg, elapsed_seconds=round(time.monotonic()-started, 3),
                      http_attempts=len(events), successes=len(successes), resources=results,
                      duplicate_successes=0, definite_window_violations=0,
                      boundary_note='Grant timestamps are bounded by public API call/callback times, not read from Redis.')
        (folder / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
        (folder / 'http-events.json').write_text(json.dumps(events, indent=2))
        (folder / 'grants.json').write_text(json.dumps(grants, indent=2))
        print(f"{'PASS' if http_within_limit else 'WARN'} {name}: {cfg['nodes']} JVMs, {len(events)} HTTP attempts, {len(successes)} successes, "
              f"0 duplicates, {report['elapsed_seconds']}s", flush=True)
        for resource, result in results.items():
            order = ['-'.join(g['operation'].split('-')[:2]) for g in sorted(grants, key=lambda g: g['after'])
                     if g['resource'] == resource][:12]
            print(f"  {resource}: each active node completed {cfg['batches']}; "
                  f"HTTP window peak={result['max_http_calls_in_window']}/{cfg['cap']}; "
                  f"uncertain grant boundaries={result['uncertain_boundary_groups']}; "
                  f"first grants: {' → '.join(order)}", flush=True)
        if not http_within_limit:
            print('  Downstream arrival window exceeded: inspect report; grant and HTTP timestamps differ.', flush=True)
        print('Logs:', folder, flush=True)
        return report
    finally:
        for p in workers:
            if p.poll() is None:
                p.terminate()
        for p in workers:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait()
        for f in handles:
            f.close()
        if redis is not None:
            redis.terminate()
            try:
                redis.wait(timeout=5)
            except subprocess.TimeoutExpired:
                redis.kill(); redis.wait()
        redis_log.close()
        server.shutdown(); server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=list(SCENARIOS) + ['all'], default='quick')
    parser.add_argument('--maven-repo', help='Optional isolated Maven dependency cache')
    parser.add_argument('--skip-build', action='store_true', help='Use an already built standalone demo')
    args = parser.parse_args()
    for tool in ['mvn', 'java', 'redis-server']:
        if not shutil.which(tool):
            parser.error('Required executable not found: ' + tool)
    java = str(Path(os.environ['JAVA_HOME']) / 'bin/java') if os.environ.get('JAVA_HOME') else shutil.which('java')
    cache = ['-Dmaven.repo.local=' + str(Path(args.maven_repo).resolve())] if args.maven_repo else []
    if not args.skip_build:
        build_log = HERE / 'target/build.log'
        build_log.parent.mkdir(exist_ok=True)
        print('Building library JAR and standalone demo; log:', build_log, flush=True)
        try:
            with build_log.open('w') as log:
                subprocess.run(['mvn', '-B', '-f', str(ROOT / 'pom.xml')] + cache + ['-DskipTests', 'install'],
                               check=True, stdout=log, stderr=subprocess.STDOUT)
                subprocess.run(['mvn', '-B', '-f', str(HERE / 'pom.xml')] + cache +
                               ['compile', 'dependency:build-classpath', '-Dmdep.outputFile=target/classpath.txt'],
                               check=True, stdout=log, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError:
            print('\n'.join(build_log.read_text().splitlines()[-30:]))
            raise

    cp = str(HERE / 'target/classes') + os.pathsep + (HERE / 'target/classpath.txt').read_text().strip()
    reports = [run_scenario(name, cp, java) for name in (SCENARIOS if args.scenario == 'all' else [args.scenario])]
    (HERE / 'target/latest-report.json').write_text(json.dumps(reports, indent=2))


if __name__ == '__main__':
    main()
