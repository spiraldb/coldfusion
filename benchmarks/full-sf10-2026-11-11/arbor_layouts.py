from pathlib import Path
import json,shlex
r=Path(__file__).resolve().parent;result={}
for suite in ['tpch','tpcds']:
 result[suite]={}
 for lane in ['spiral-sdk','spiral-direct']:
  p=r/(suite+'-'+lane+'.log')
  if not p.exists():continue
  rows=[]
  for line in p.read_text().splitlines():
   if line.startswith('payload_fingerprint '):rows.append(dict(item.split('=',1) for item in shlex.split(line)[1:]))
  result[suite][lane]=dict(tables=rows,files=sum(int(x['files']) for x in rows),bytes=sum(int(x['bytes']) for x in rows))
 if len(result[suite])==2:
  sdk={x['table']:x for x in result[suite]['spiral-sdk']['tables']};direct={x['table']:x for x in result[suite]['spiral-direct']['tables']}
  result[suite]['encoded_payloads_match']=sdk==direct
(r/'arbor-layout-receipts.json').write_text(json.dumps(result,indent=2))
print(json.dumps({s:{l:({k:v for k,v in x.items() if k!='tables'} if isinstance(x,dict) else x) for l,x in lanes.items()} for s,lanes in result.items()},indent=2))
