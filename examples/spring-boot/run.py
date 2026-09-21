#!/usr/bin/env python3
"""Run three packaged Spring Boot consumers against an isolated authenticated Redis."""
import argparse
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PASSWORD = 'local-test-only'


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def redis_command(endpoint, *args, database=0):
    with socket.create_connection(('127.0.0.1', endpoint), 1) as s:
        s.settimeout(1)
        f = s.makefile('rb')
        def call(parts):
            data = b'*%d\r\n' % len(parts)
            for part in parts:
                value = str(part).encode()
                data += b'$%d\r\n' % len(value) + value + b'\r\n'
            s.sendall(data)
            line = f.readline()
            if not line: raise OSError('Redis disconnected')
            if line[:1] == b'-': raise OSError(line[1:-2].decode())
            if line[:1] == b':': return int(line[1:-2])
            return line[1:-2].decode()
        call(['AUTH', PASSWORD])
        call(['SELECT', database])
        return call(args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--boot-version', default='3.5.16')
    parser.add_argument('--maven-repo')
    parser.add_argument('--skip-build', action='store_true')
    args = parser.parse_args()
    for tool in ['java', 'redis-server'] + ([] if args.skip_build else ['mvn']):
        if not shutil.which(tool): parser.error('Missing ' + tool)
    java = str(Path(os.environ['JAVA_HOME'])/'bin/java') if os.environ.get('JAVA_HOME') else shutil.which('java')
    target = HERE/'target'
    target.mkdir(exist_ok=True)
    cache = ['-Dmaven.repo.local='+str(Path(args.maven_repo).resolve())] if args.maven_repo else []
    if not args.skip_build:
        with (target/'build.log').open('w') as log:
            for pom, extra in [(ROOT/'pom.xml', ['-DskipTests','install']),
                               (ROOT/'spring-boot-starter/pom.xml', ['-DskipTests','install']),
                               (HERE/'pom.xml', ['-Dspring-boot.version='+args.boot_version,'package'])]:
                subprocess.run(['mvn','-B','-f',str(pom)]+cache+extra,stdout=log,stderr=subprocess.STDOUT,check=True)
    jar = target/('boot-'+args.boot_version)/'fair-grant-boot-demo-1.0.0-SNAPSHOT.jar'
    folder = target/('run-'+str(time.time_ns()));folder.mkdir()
    redis_port = port()
    calls = []
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.rfile.read(int(self.headers['Content-Length'])).decode())
            self.send_response(204);self.end_headers()
        def log_message(self,*args): pass
    server = http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    workers=[];logs=[];redis=None
    try:
        redis_log=(folder/'redis.log').open('w');logs.append(redis_log)
        redis=subprocess.Popen(['redis-server','--bind','127.0.0.1','--port',str(redis_port),
                '--requirepass',PASSWORD,'--save','','--appendonly','no','--dir',str(folder)],stdout=redis_log,stderr=redis_log)
        deadline=time.monotonic()+10
        while True:
            try:
                if redis_command(redis_port,'PING')=='PONG':break
            except OSError:
                if redis.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Redis startup failed')
                time.sleep(.02)
        prefix='boot-demo:'+uuid.uuid4().hex+':'
        for node in ['a','b','c']:
            log=(folder/(node+'.log')).open('w');logs.append(log)
            workers.append(subprocess.Popen([java,'-Xmx128m','-jar',str(jar),
                '--fair-grant.redis.port='+str(redis_port),'--fair-grant.redis.password='+PASSWORD,
                '--fair-grant.redis.username=default','--fair-grant.redis.database=2',
                '--fair-grant.key-prefix='+prefix,'--demo.node='+node,'--demo.run-dir='+str(folder),
                '--demo.url=http://127.0.0.1:'+str(server.server_port)+'/submit'],stdout=log,stderr=log))
        deadline=time.monotonic()+40
        while len(list(folder.glob('*.ready')))!=3:
            if any(p.poll() is not None for p in workers) or time.monotonic()>deadline:
                raise RuntimeError('Boot startup failed; see '+str(folder))
            time.sleep(.05)
        identities=[p.read_text() for p in folder.glob('*.ready')]
        assert len(set(identities))==3, 'Default instance IDs collided'
        (folder/'go').touch()
        for p in workers:
            assert p.wait(timeout=45)==0, 'Boot consumer failed; see '+str(folder)
        grants=[]
        for node in ['a','b','c']:
            text=(folder/(node+'.log')).read_text()
            assert 'BOOT_VERSION '+args.boot_version in text, 'Wrong Boot runtime'
            assert 'COMPLETE '+node in text
            for line in text.splitlines():
                if line.startswith('GRANT '):
                    _,client,task,before,after=line.split()
                    grants.append(dict(client=client,task=int(task),before=int(before),after=int(after)))
        assert len(calls)==9 and len(set(calls))==9 and len(grants)==9, 'Missing or duplicate business operation'
        grants.sort(key=lambda g:g['after'])
        assert len({g['client'] for g in grants[:3]})==3, 'Initial FIFO turn skipped a ready node'
        uncertain=0
        for i in range(4,len(grants)):
            group=grants[i-4:i+1]
            assert max(g['after'] for g in group)-min(g['before'] for g in group)>=2000000, 'Definite window violation'
            uncertain+=max(g['before'] for g in group)-min(g['after'] for g in group)<2000000
        assert redis_command(redis_port,'DBSIZE',database=0)==0, 'Configured database was ignored'
        assert redis_command(redis_port,'DBSIZE',database=2)>0
        with zipfile.ZipFile(jar) as z:
            libs=[n for n in z.namelist() if n.startswith('BOOT-INF/lib/jedis-')]
        report=dict(boot=args.boot_version,java=subprocess.run([java,'-version'],capture_output=True,text=True).stderr,
                    jedis=libs,nodes=3,operations=9,distinct_instance_ids=identities,authenticated=True,database=2,
                    initial_fifo_turn=True,definite_window_violations=0,uncertain_window_groups=uncertain,grants=grants)
        (folder/'report.json').write_text(json.dumps(report,indent=2))
        (target/'latest-report.json').write_text(json.dumps(report,indent=2))
        print('PASS Boot '+args.boot_version+': 3 JVMs, 9 HTTP operations, authenticated Redis DB 2, FIFO, context shutdown; '+str(libs))
        print('Report:',folder/'report.json')
    finally:
        for p in workers:
            if p.poll() is None:p.terminate()
        for p in workers:
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        if redis is not None and redis.poll() is None:
            redis.terminate()
            try:redis.wait(timeout=10)
            except subprocess.TimeoutExpired:redis.kill();redis.wait()
        for log in logs:log.close()
        server.shutdown();server.server_close()

if __name__=='__main__':main()
