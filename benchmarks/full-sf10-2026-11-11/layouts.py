from pathlib import Path
import json
r=Path(__file__).resolve().parent
manifest={}
for suite in ['tpch','tpcds']:
 formats={}
 for lane in ['coldfusion-vortex','coldfusion-lz4']:
  p=r/(suite+'-'+lane+'.log')
  if not p.exists():continue
  tables=[]
  for line in p.read_text().splitlines():
   if line.startswith(('coldfusion_vortex_layout=','coldfusion_parquet_layout=')):
    x=json.loads(line.split('=',1)[1]);tables.append(x)
  formats[lane]=dict(tables=tables,output_total_bytes=sum(x['output_total_bytes'] for x in tables),files=sum(len(x['files']) for x in tables))
 manifest[suite]=formats
(r/'layout-receipts.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps({s:{f:{k:v for k,v in x.items() if k!='tables'} for f,x in m.items()} for s,m in manifest.items()},indent=2))
