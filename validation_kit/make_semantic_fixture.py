#!/usr/bin/env python3
"""Generate isolated SQL fixtures. Does not connect to the cluster."""
import json,pathlib
from validate import OUTPUT,EXPRESSIONS,load_config,lit
c=load_config('config.json');db=c['output_database'];prefix=c['run_id']
src=f'{db}.{prefix}_fixture_source';req=f'{db}.{prefix}_fixture_requests'
# Original source schema: output first 25 fields plus three source fields and date.
source_cols=OUTPUT[:25]+[('tethering','STRING'),('application_name','STRING'),('subs_circle','STRING'),('date_partition','DATE')]
reqcols=[('report_value','STRING'),('incident_number','STRING'),('req_id','STRING'),('from_date','TIMESTAMP'),('to_date','TIMESTAMP'),('num_type','STRING')]
k1='00000000000001';k2='00000000000002'
requests=[(k1,'case1','r1','2026-10-11 10:00:00','2026-10-11 11:00:00','IMEI'),
(k1,'case2','r2','2026-10-11 10:00:00','2026-10-11 11:00:00','IMEI'),
(k1,'case3','r3','2026-10-13 09:00:00','2026-10-13 11:00:00','IMEI'),
(k2,'case4','r4','2026-10-11 10:00:00','2026-10-11 11:00:00','IMEI'),
(k1,'null','r5',None,'2026-10-11 11:00:00','IMEI'),
(k1,'invalid','r6','2026-10-11 12:00:00','2026-10-11 08:00:00','IMEI')]
rows=[('inside',k1,'2026-10-11 10:15:00','2026-10-11 10:30:00'),
('inside',k1,'2026-10-11 10:15:00','2026-10-11 10:30:00'),
('boundary',k1,'2026-10-11 09:00:00','2026-10-11 10:00:00'),
('covers',k1,'2026-10-11 09:00:00','2026-10-11 12:00:00'),
('null_end',k1,'2026-10-11 10:20:00',None),
('null_start',k1,None,'2026-10-11 10:20:00'),
('outside',k1,'2026-10-11 08:00:00','2026-10-11 09:00:00'),
('cross_date',k1,'2026-10-11 23:00:00','2026-10-13 10:00:00'),
('leading_zero',k2,'2026-10-11 10:20:00','2026-10-11 10:30:00')]
def val(v,t):
 if v is None:return 'NULL'
 return ('CAST('+lit(v)+' AS '+t+')') if t in ('DATE','TIMESTAMP') else lit(v)
def ddl(name,cols):return 'CREATE TABLE '+name+' ('+', '.join(n+' '+t for n,t in cols)+") STORED AS ICEBERG;\n"
sql='CREATE DATABASE IF NOT EXISTS '+db+';\n'+ddl(src,source_cols)+ddl(req,reqcols)
sql+='INSERT INTO '+req+' VALUES\n'+',\n'.join('('+', '.join(val(v,t) for v,(_,t) in zip(r,reqcols))+')' for r in requests)+';\n'
values=[]
for label,key,start,end in rows:
 d={'imsi':label,'imei_14':key,'session_start_date_time':start,'session_end_date_time':end,'date_partition':'2026-10-11'}
 values.append('('+', '.join(val(d.get(n),t) for n,t in source_cols)+')')
sql+='INSERT INTO '+src+' VALUES\n'+',\n'.join(values)+';\n'
pred="a.imei_14=b.report_value AND b.num_type='IMEI' AND a.date_partition=DATE '2026-10-11' AND (a.session_start_date_time BETWEEN b.from_date AND b.to_date OR a.session_end_date_time BETWEEN b.from_date AND b.to_date)"
sql+=f'SELECT COUNT(*) AS expected_12 FROM {src} a JOIN {req} b ON {pred};\n'
sql+=f'SELECT a.imsi, COUNT(*) AS rows FROM {src} a JOIN {req} b ON {pred} GROUP BY a.imsi ORDER BY a.imsi;\n'
p=pathlib.Path('semantic_fixture.sql');p.write_text(sql)
fixture=dict(c);fixture.update(source_table=src,request_table=req,run_id=prefix+'_fixture',test_date='2026-10-11',full_dates=['2026-10-11'])
fixture['expected_day_rows']=12;fixture['expected_full_rows']=12
fixture['baseline_options']=dict(c['baseline_options']);fixture['baseline_options']['MT_DOP']='1';fixture['baseline_options']['MEM_LIMIT']='2g'
pathlib.Path('config.fixture.json').write_text(json.dumps(fixture,indent=2))
print('Created semantic_fixture.sql (expected output: 12 rows) and config.fixture.json. Run the SQL once; no tables are dropped.')
