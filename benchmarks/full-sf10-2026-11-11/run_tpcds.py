from pathlib import Path
import json,subprocess,os,time,hashlib,statistics,datetime
r=Path(__file__).resolve().parent
binary=Path('/Users/danielking/projects/coldfusion/spiraldb/target/release/engine-bench')
env=os.environ.copy();env.update(ENGINE_BENCH_SKIP_GEN='1',TOKIO_WORKER_THREADS='14')
lanes=[('spiral-sdk','arbor','spiral-datafusion'),('coldfusion-vortex','coldfusion-vortex','datafusion'),('coldfusion-parquet','coldfusion','datafusion'),('coldfusion-lz4','coldfusion-lz4-raw','datafusion'),('spiral-direct','arbor','datafusion')]
results=[]
for lane,fmt,engine in lanes:
 run='tpcds-'+lane
 args=[str(binary),'tpcds','--iterations','3','--warmups','1','--formats','parquet','--hide-progress-bar','--threads','14','--opt','scale-factor=10','--opt','remote-data-dir=file:///Users/danielking/projects/coldfusion/bench-cache/vortex/vortex-bench/data/tpcds/10/','--query-engine',engine,'--sql-timeout-secs','120','--table-prepare-timeout-secs','300','--table-format',fmt,'--runner','local-m3max-14','--run-id','sf10-generic-'+run,'--gh-json-v3',str(r/(run+'.jsonl'))]
 receipt=dict(argv=args,cwd='/Users/danielking/projects/coldfusion/bench-cache/vortex',env={'ENGINE_BENCH_SKIP_GEN':'1','TOKIO_WORKER_THREADS':'14'},binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),provider_commit='822758c01467176b516df8e9e6fbc4301ccc1b9d',production_code_commit='34045d5b61bdc927fe27d709b522008fda3fcd11',started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
 (r/(run+'-command.json')).write_text(json.dumps(receipt,indent=2))
 print('START',run,flush=True); start=time.time()
 with (r/(run+'.log')).open('w') as out:
  child=subprocess.Popen(args,cwd=receipt['cwd'],env=env,stdout=out,stderr=subprocess.STDOUT)
  (r/'active.json').write_text(json.dumps(dict(pid=child.pid,lane=run,started=time.time())))
  code=child.wait()
 rows=[json.loads(line) for line in (r/(run+'.jsonl')).read_text().splitlines() if 'query_coverage' in line] if (r/(run+'.jsonl')).exists() else []
 good=[x for x in rows if x['status']=='passed']
 summary=dict(lane=run,returncode=code,seconds=time.time()-start,count=len(rows),passed=len(good),sum_means_seconds=sum(statistics.mean(x['all_runtimes_ns'])/1e9 for x in good if x['all_runtimes_ns']),failed=[dict(query=x['query_idx'],status=x['status'],reason=x.get('reason'),detail=x.get('detail')) for x in rows if x['status']!='passed'])
 results.append(summary);(r/'tpcds-summary.json').write_text(json.dumps(results,indent=2));print('DONE',json.dumps(summary),flush=True)
(r/'active.json').unlink(missing_ok=True)
