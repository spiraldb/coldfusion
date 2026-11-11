from pathlib import Path
import json,subprocess,os,time,signal,shutil
r=Path(__file__).resolve().parent
binary='/Users/danielking/projects/coldfusion/spiraldb/target/release/engine-bench'
data=Path('/Users/danielking/projects/coldfusion/bench-cache/vortex/vortex-bench/data/tpcds/10')
cwd='/Users/danielking/projects/coldfusion/bench-cache/vortex'
env=os.environ.copy();env['TOKIO_WORKER_THREADS']='14';env.pop('ENGINE_BENCH_SKIP_GEN',None)
common=[binary,'tpcds','--iterations','1','--warmups','0','--formats','vortex','--hide-progress-bar','--threads','14','--opt','scale-factor=10','--opt',data.as_uri().replace('file:','remote-data-dir=file:',1)+'/', '--generate-sql-answers','--sql-answers-only','--sql-timeout-secs','120']
# First process generates the canonical Parquet/Vortex dataset and reference Q1.
args=common+['--queries','1']
(r/'tpcds-generate-command.json').write_text(json.dumps(dict(argv=args,cwd=cwd,env={'TOKIO_WORKER_THREADS':'14'}),indent=2))
with (r/'tpcds-generate.log').open('w') as out:
 p=subprocess.Popen(args,cwd=cwd,env=env,stdout=out,stderr=subprocess.STDOUT)
 (r/'active.json').write_text(json.dumps(dict(pid=p.pid,lane='tpcds-generation',started=time.time())))
 reference_started=None
 while p.poll() is None:
  converted=data/'vortex-file-compressed'
  if converted.exists() and len(list(converted.glob('*_0001.vortex')))>=24:
   if reference_started is None: reference_started=time.monotonic()
   if time.monotonic()-reference_started>=120:
    p.kill();p.wait();raise RuntimeError('Q1 reference reached 120s; dataset conversion finished')
  if shutil.disk_usage(data if data.exists() else data.parent).free < 1024**3:
   p.kill();p.wait();raise RuntimeError('Stopped owned data generator before exhausting local disk')
  time.sleep(2)
 assert p.returncode==0,'data/reference Q1 generation failed: see tpcds-generate.log'
print('DATA_READY',flush=True)
# Reference generation is isolated from benchmark timings. Completed answers have
# fingerprints + telemetry; a stalled query is killed after 120 seconds with no
# completed reference, before proceeding to other reference queries.
env['ENGINE_BENCH_SKIP_GEN']='1'
failed=[]; attempt=0
answers=data/'answers'
def complete(q):return all((answers/f'q{q}.{suffix}').exists() for suffix in ['csv','csv.meta','nulls-min.telemetry.json','nulls-min.plan.txt'])
remaining=[q for q in range(1,100) if not complete(q)]
while remaining:
 attempt+=1;args=common+['--queries',','.join(map(str,remaining))]
 stem='tpcds-reference-'+str(attempt)
 (r/(stem+'-command.json')).write_text(json.dumps(dict(argv=args,cwd=cwd,env={'TOKIO_WORKER_THREADS':'14','ENGINE_BENCH_SKIP_GEN':'1'},watchdog_seconds_without_completed_reference=120),indent=2))
 with (r/(stem+'.log')).open('w') as out:
  p=subprocess.Popen(args,cwd=cwd,env=env,stdout=out,stderr=subprocess.STDOUT)
  (r/'active.json').write_text(json.dumps(dict(pid=p.pid,lane=stem,started=time.time())))
  last=time.monotonic(); pending=remaining.copy(); timedout=False
  while p.poll() is None:
   now_pending=[q for q in pending if not complete(q)]
   if len(now_pending)!=len(pending):
    done=[q for q in pending if q not in now_pending];print('REFERENCE_READY',done,flush=True);last=time.monotonic();pending=now_pending
   if pending and time.monotonic()-last>=120:
    p.kill();p.wait();timedout=True;break
   time.sleep(1)
 pending=[q for q in remaining if not complete(q)]
 if pending:
  q=pending[0];failed.append(dict(query=q,kind='timeout' if timedout else 'error',log=stem+'.log'));print('REFERENCE_FAILED',failed[-1],flush=True)
 remaining=[q for q in pending if q not in {x['query'] for x in failed}]
 (r/'tpcds-reference-status.json').write_text(json.dumps(dict(ready=[q for q in range(1,100) if complete(q)],failed=failed),indent=2))
(r/'active.json').unlink(missing_ok=True)
print('REFERENCES_DONE',json.dumps(dict(ready=sum(complete(q) for q in range(1,100)),failed=failed)),flush=True)
