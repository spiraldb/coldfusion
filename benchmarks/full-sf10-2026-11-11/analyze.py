from pathlib import Path
import json,statistics
r=Path(__file__).resolve().parent
lanes=['spiral-sdk','spiral-direct','coldfusion-vortex','coldfusion-parquet','coldfusion-lz4']
results={}
for suite,n in [('tpch',22),('tpcds',99)]:
 result={}
 for lane in lanes:
  p=r/(suite+'-'+lane+'.jsonl')
  if not p.exists():continue
  rows=[json.loads(l) for l in p.read_text().splitlines() if 'query_coverage' in l]
  good=[x for x in rows if x['status']=='passed' and x.get('all_runtimes_ns')]
  result[lane]=dict(count=len(rows),expected=n,passed=len(good),sum_query_means_seconds=sum(statistics.mean(x['all_runtimes_ns'])/1e9 for x in good),validation_labels={kind:sum(x.get('answer_match')==kind for x in rows) for kind in ['exact','approximate']},failed=[x for x in rows if x['status']!='passed'],queries={str(x['query_idx']):dict(mean_ns=statistics.mean(x['all_runtimes_ns']),samples_ns=x['all_runtimes_ns'],status=x['status'],answer_match=x.get('answer_match'),result_digest=x.get('result_digest')) for x in good})
 results[suite]=result
print(json.dumps({suite:{lane:{key:value for key,value in result.items() if key not in ['queries','failed']} for lane,result in lanes.items()} for suite,lanes in results.items()},indent=2))
(r/'analysis.json').write_text(json.dumps(results,indent=2))
