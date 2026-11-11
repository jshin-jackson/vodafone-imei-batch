#!/usr/bin/env python3
"""Impala validation helper. Standard library only; no remote work until a command is invoked.
Never modifies source/request tables or drops/truncates outputs.
"""
import argparse, csv, datetime as dt, hashlib, io, json, pathlib, random, re, subprocess, sys, time

OUTPUT = [
 ('imsi','STRING'),('imei','STRING'),('imei_14','STRING'),('msisdn','STRING'),
 ('mac_address','STRING'),('source_ip','STRING'),('source_port','STRING'),
 ('destination_ip','STRING'),('destination_port','STRING'),('translated_ip','STRING'),
 ('translated_port','STRING'),('first_cell_id_name_location','STRING'),
 ('session_start_date_time','TIMESTAMP'),('session_end_date_time','TIMESTAMP'),
 ('duration_in_sec','STRING'),('data_volume_uplink','STRING'),('data_volume_downlink','STRING'),
 ('pgw_ip_address','STRING'),('charging_id','STRING'),('access_point_name','STRING'),
 ('pdp_address_ipv4','STRING'),('pdp_address_ipv6','STRING'),('cgi_id','STRING'),
 ('rat','STRING'),('pdp_type','STRING'),('tethering','STRING'),('application_name','STRING'),
 ('ipdr_filename','STRING'),('cgf_filename','STRING'),('cgnat_filename','STRING'),
 ('roaming_nw_ceid','STRING'),('first_cellid_name_address','STRING'),
 ('subs_circle','STRING'),('subs_roam_circle','STRING'),('date_partition','DATE'),
 ('incident_number','STRING'),('req_id','STRING')]
EXPRESSIONS = ['a.'+n for n,_ in OUTPUT[:25]] + [
 "'N'", "'N'", "'NA'", "'NA'", "'NA'", 'a.cgi_id', "''", "'NA'", "'NA'",
 'a.date_partition','b.incident_number','b.req_id']
EXPRESSIONS[14]='CAST(a.duration_in_sec AS STRING)'
assert len(EXPRESSIONS)==len(OUTPUT)==37
CASE_OVERRIDES={
 'baseline':{}, 'mem8':{'MEM_LIMIT':'8g'}, 'mem6':{'MEM_LIMIT':'6g'},
 'A':{'PARQUET_LATE_MATERIALIZATION_THRESHOLD':'-1'},
 'B':{'PARQUET_LATE_MATERIALIZATION_THRESHOLD':'-1'},
 'C':{'PARQUET_LATE_MATERIALIZATION_THRESHOLD':'20'},
 'D':{'PARQUET_LATE_MATERIALIZATION_THRESHOLD':'20'},
 'static_current':{},'writer8':{'MAX_FS_WRITERS':'8'},'writer1':{'MAX_FS_WRITERS':'1'},
 'dop2':{'MT_DOP':'2'},'dop5':{'MT_DOP':'5'},'final_base':{},'final_candidate':{}}
STATIC={'B','D','static_current','final_candidate'}

def ident(s,qualified=True):
 pattern=r'[A-Za-z_][A-Za-z0-9_]*'+(r'\.[A-Za-z_][A-Za-z0-9_]*' if qualified else '')
 if not isinstance(s,str) or not re.fullmatch(pattern,s):raise ValueError('Invalid identifier: '+str(s))
 return s

def lit(s):
 return "'"+str(s).replace('\\','\\\\').replace("'","\\'")+"'"

def datecheck(s):dt.date.fromisoformat(s);return s

def load_config(path):
 c=json.loads(pathlib.Path(path).read_text())
 for k in ('source_table','request_table'):ident(c[k])
 for k in ('output_database','run_id'):ident(c[k],False)
 datecheck(c['test_date'])
 if not c['full_dates']:raise ValueError('full_dates cannot be empty')
 for d in c['full_dates']:datecheck(d)
 if c['test_date'] not in c['full_dates']:raise ValueError('test_date must be in full_dates')
 if not isinstance(c.get('shell_args'),list):raise ValueError('shell_args must be a list')
 if any(x in c['shell_args'] for x in ('-p','--show_profiles','-q','-f','-o','-B','--print_header')):
  raise ValueError('Do not put query/output/profile flags in shell_args')
 return c

def settings(c,case):
 o=dict(c['baseline_options']);o.update(c.get('case_overrides',{}).get(case,{}));o.update(CASE_OVERRIDES[case])
 # final_candidate can explicitly adopt a tested combination.
 if case=='final_candidate':o.update(c.get('final_candidate_options',{}))
 return o

def settings_sql(o):
 out=[]
 for k,v in o.items():
  if not re.fullmatch('[A-Z][A-Z0-9_]*',k):raise ValueError('Invalid option '+k)
  v=str(v)
  if not re.fullmatch(r'[A-Za-z0-9_./:+-]+',v):raise ValueError('Invalid option value '+v)
  out.append(f'SET {k}={lit(v)};')
 return '\n'.join(out)

class Lab:
 def __init__(self,args):
  self.args=args;self.c=load_config(args.config)
  self.root=pathlib.Path(args.artifacts)/self.c['run_id'];self.root.mkdir(parents=True,exist_ok=True)
 def shell(self,sql,name,delimited=False):
  sqlpath=self.root/(name+'.sql');sqlpath.write_text(sql)
  cmd=[self.c['impala_shell']]+self.c['shell_args']
  if delimited:cmd+=['-B','--output_delimiter=\t']
  cmd+=['-f',str(sqlpath)]
  start=time.monotonic()
  if delimited:
   r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
   (self.root/(name+'.tsv')).write_text(r.stdout)
   (self.root/(name+'.stderr.log')).write_text(r.stderr)
  else:
   with (self.root/(name+'.log')).open('w') as log:
    r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,text=True)
  elapsed=time.monotonic()-start
  if r.returncode:raise RuntimeError(f'{name} failed (exit {r.returncode}). Inspect {self.root}; no automatic retry.')
  if delimited:return list(csv.reader(io.StringIO(r.stdout),delimiter='\t'))
  return elapsed
 def frozen(self):
  f=json.loads((self.root/'frozen.json').read_text())
  for k in ('source_table','request_table','test_date','full_dates'):
   if self.c[k]!=f[k]:raise ValueError(k+' changed: start a NEW run_id and freeze again')
  if hashlib.sha256(json.dumps(f['imeis'],ensure_ascii=False).encode()).hexdigest()!=f['imei_sha256']:
   raise ValueError('Frozen IMEI list checksum mismatch')
  return f
 def source(self,f):return f"(SELECT * FROM {f['source_table']} FOR SYSTEM_VERSION AS OF {f['source_snapshot']})"
 def request(self,f):return f"(SELECT * FROM {f['request_table']} FOR SYSTEM_VERSION AS OF {f['request_snapshot']})"
 def datepredicate(self,full=False):
  return ('a.date_partition IN ('+', '.join("DATE "+lit(x) for x in self.c['full_dates'])+')' if full
          else 'a.date_partition = DATE '+lit(self.c['test_date']))
 def select(self,f,static=False,full=False):
  q='SELECT\n  '+',\n  '.join(EXPRESSIONS)+f'\nFROM {self.source(f)} a\nJOIN {self.request(f)} b\n  ON a.imei_14=b.report_value\nWHERE '+self.datepredicate(full)+"\n  AND b.num_type='IMEI'\n  AND (a.session_start_date_time BETWEEN b.from_date AND b.to_date\n       OR a.session_end_date_time BETWEEN b.from_date AND b.to_date)"
  if static:q+='\n  AND a.imei_14 IN ('+', '.join(lit(x) for x in f['imeis'])+')'
  return q
 def target(self,case,repeat):
  if case not in CASE_OVERRIDES:raise ValueError('Unknown case')
  if not re.fullmatch('[a-zA-Z0-9_]+',str(repeat)):raise ValueError('Invalid repeat')
  return f"{self.c['output_database']}.{self.c['run_id']}_{case.lower()}_r{repeat}"
 def generate(self,case,repeat):
  f=self.frozen();target=self.target(case,repeat)
  if target.lower() in (self.c['source_table'].lower(),self.c['request_table'].lower()):raise ValueError('Unsafe target')
  use_static = (self.c.get('final_candidate_static',True) if case=='final_candidate' else case in STATIC or case in self.c.get('static_cases',[]))
  opts=settings_sql(settings(self.c,case));query='INSERT INTO '+target+'\n'+self.select(f,use_static,case.startswith('final_'))+';'
  ddl='CREATE TABLE '+target+' (\n'+',\n'.join('  '+n+' '+t for n,t in OUTPUT)+"\n) STORED AS ICEBERG\nTBLPROPERTIES ('write.format.default'='parquet');\n"
  stem=f'{case}_r{repeat}'
  for suffix,sql in [('create',ddl),('explain',opts+'\nSET EXPLAIN_LEVEL=3;\nEXPLAIN '+query),
      ('run',opts+'\n'+query+'\nSUMMARY;\nPROFILE;\n')]:
   (self.root/(stem+'_'+suffix+'.sql')).write_text(sql)
  return stem,target
 def inspect(self):
  c=self.c
  sql='SELECT VERSION();\nSET ALL;\n'
  for t in (c['source_table'],c['request_table']):
   sql+=f'SHOW CREATE TABLE {t};\nDESCRIBE FORMATTED {t};\nSHOW TABLE STATS {t};\nSHOW COLUMN STATS {t};\n'
  self.shell(sql,'00_environment')
  self.shell(f"SELECT spec_id, content, COUNT(*) AS files, SUM(file_size_in_bytes) AS bytes, AVG(file_size_in_bytes) AS avg_bytes, SUM(CASE WHEN file_size_in_bytes<67108864 THEN 1 ELSE 0 END) AS files_lt_64m FROM {c['source_table']}.files GROUP BY spec_id,content;",'01_files')
 def freeze(self):
  dst=self.root/'frozen.json'
  if dst.exists():raise ValueError('frozen.json already exists. Use a new run_id; do not overwrite inputs.')
  f={k:self.c[k] for k in ('source_table','request_table','test_date','full_dates')}
  for role in ('source','request'):
   rows=self.shell('DESCRIBE FORMATTED '+self.c[role+'_table']+';',f'02_{role}_metadata',True)
   values=[]
   for row in rows:
    for i,v in enumerate(row[:-1]):
     if v.strip()=='current-snapshot-id' and re.fullmatch(r'\d+',row[i+1].strip()):values.append(row[i+1].strip())
   if len(set(values))!=1:raise ValueError('Cannot identify current-snapshot-id. Inspect metadata output; do not use latest committed timestamp as a substitute.')
   f[role+'_snapshot']=int(values[0])
  # Query the pinned snapshot, not the current request table.
  rows=self.shell(f"SELECT DISTINCT report_value FROM {f['request_table']} FOR SYSTEM_VERSION AS OF {f['request_snapshot']} WHERE num_type='IMEI' AND report_value IS NOT NULL ORDER BY report_value;",'03_pinned_imeis',True)
  values=[r[0] for r in rows if len(r)==1 and r[0]!='']
  if not values or any(not re.fullmatch(r'\d+',v) for v in values):raise ValueError('No IMEIs, header/format mismatch or non-digit IMEI: inspect TSV; do not drop values silently.')
  f['imeis']=sorted(set(values));f['imei_sha256']=hashlib.sha256(json.dumps(f['imeis'],ensure_ascii=False).encode()).hexdigest()
  f['frozen_at_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
  dst.write_text(json.dumps(f,ensure_ascii=False,indent=2))
  print('Frozen:',f['source_snapshot'],f['request_snapshot'],'distinct IMEIs:',len(f['imeis']))
 def quality(self):
  f=self.frozen();src=self.source(f);req=self.request(f);pred=self.datepredicate()
  sql=f"""SELECT COUNT(*) AS request_rows, COUNT(DISTINCT report_value) AS distinct_imeis,
 COUNT(DISTINCT req_id) AS distinct_requests, MIN(from_date), MAX(to_date),
 SUM(CASE WHEN from_date IS NULL OR to_date IS NULL THEN 1 ELSE 0 END) AS null_periods,
 SUM(CASE WHEN from_date>to_date THEN 1 ELSE 0 END) AS invalid_periods
FROM {req} b WHERE num_type='IMEI';
SELECT COUNT(*) AS day_rows, NDV(imei_14) AS imei_ndv_estimate,
 SUM(CASE WHEN imei_14 IS NULL THEN 1 ELSE 0 END) AS null_imei,
 SUM(CASE WHEN session_start_date_time IS NULL THEN 1 ELSE 0 END) AS null_start,
 SUM(CASE WHEN session_end_date_time IS NULL THEN 1 ELSE 0 END) AS null_end,
 SUM(CASE WHEN session_start_date_time>session_end_date_time THEN 1 ELSE 0 END) AS reversed_times
FROM {src} a WHERE {pred};
SELECT report_value, COUNT(*) AS requests_per_imei FROM {req} b
WHERE num_type='IMEI' GROUP BY report_value ORDER BY requests_per_imei DESC LIMIT 20;
SELECT COUNT(*) AS matched_output_rows FROM {src} a JOIN {req} b ON a.imei_14=b.report_value WHERE {pred} AND b.num_type='IMEI' AND (a.session_start_date_time BETWEEN b.from_date AND b.to_date OR a.session_end_date_time BETWEEN b.from_date AND b.to_date);
"""
  self.shell(sql,'04_quality_not_a_benchmark')
 def plan(self,case,repeat):
  stem,_=self.generate(case,repeat)
  self.shell((self.root/(stem+'_explain.sql')).read_text(),stem+'_plan')
 def run(self,case,repeat):
  stem,target=self.generate(case,repeat);manifest=self.root/(stem+'_manifest.json')
  if manifest.exists():raise ValueError('Run already attempted. Use a new repeat label; inspect prior state.')
  state={'case':case,'repeat':str(repeat),'table':target,'options':settings(self.c,case),
         'date_scope':'full_dates' if case.startswith('final_') else 'test_date','status':'started',
         'started_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
         'frozen_input':{k:self.frozen()[k] for k in ('source_snapshot','request_snapshot','imei_sha256')}}
  manifest.write_text(json.dumps(state,indent=2))
  try:
   self.shell('CREATE DATABASE IF NOT EXISTS '+self.c['output_database']+';',stem+'_database')
   # No IF NOT EXISTS: stale output must fail, never append silently.
   self.shell((self.root/(stem+'_create.sql')).read_text(),stem+'_create_log')
   elapsed=self.shell((self.root/(stem+'_run.sql')).read_text(),stem+'_profile')
   rows=self.shell('SELECT COUNT(*) FROM '+target+';',stem+'_output_count',True)
   if len(rows)!=1 or len(rows[0])!=1:raise ValueError('Unexpected output count format')
   count=int(rows[0][0]);state.update(output_rows=count)
   expected=self.c.get('expected_full_rows' if case.startswith('final_') else 'expected_day_rows')
   if expected is not None and count!=int(expected):raise ValueError(f'Output count {count} differs from expected {expected}. Stop and inspect data/snapshot/date.')
   state.update(status='finished',client_process_seconds=elapsed)
  except Exception as e:
   state.update(status='failed',error=str(e));raise
  finally:manifest.write_text(json.dumps(state,indent=2))
  print('Finished',target,'client process seconds',round(elapsed,3))
 def compare(self,left,right):
  # Inputs are case:repeat labels, never arbitrary table names.
  lc,lr=left.split(':');rc,rr=right.split(':');a=self.target(lc,lr);b=self.target(rc,rr)
  for case,rep in ((lc,lr),(rc,rr)):
   m=json.loads((self.root/f'{case}_r{rep}_manifest.json').read_text())
   if m['status']!='finished':raise ValueError('Only completed runs can be compared')
  if lc.startswith('final_')!=rc.startswith('final_'):raise ValueError('Cannot compare one-day and full-window runs')
  cols=', '.join(n for n,_ in OUTPUT)
  sql=f"""WITH combined AS (
 SELECT {cols}, CAST(1 AS BIGINT) AS weight FROM {a}
 UNION ALL
 SELECT {cols}, CAST(-1 AS BIGINT) AS weight FROM {b}
), differences AS (
 SELECT {cols}, SUM(weight) AS delta FROM combined
 GROUP BY {cols}
 HAVING SUM(weight)<>0
)
SELECT COUNT(*) AS mismatched_value_groups,
 COALESCE(SUM(ABS(delta)),0) AS differing_row_multiplicity
FROM differences;
"""
  name='compare_'+left.replace(':','_')+'_vs_'+right.replace(':','_')
  rows=self.shell(sql,name,True)
  if len(rows)!=1 or rows[0]!=['0','0']:raise RuntimeError('RESULT MISMATCH or unexpected output. Inspect '+name+'.tsv')
  print('PASS: exact multiset equality across all 37 columns, including NULLs and duplicate counts.')
 def summarize(self):
  results=[]
  for p in sorted(self.root.glob('*_manifest.json')):
   m=json.loads(p.read_text());case=m['case'];rep=m['repeat']
   log=self.root/f'{case}_r{rep}_profile.log';s=log.read_text() if log.exists() else ''
   out=dict(case=case,repeat=rep,status=m['status'],table=m['table'],client_process_seconds=m.get('client_process_seconds',''))
   for key,pattern in [('query_duration',r'^\s*Duration:\s*(.+)$'),('total_bytes_read',r'^\s*- TotalBytesRead:\s*(.+)$'),('total_cpu',r'^\s*- TotalCpuTime:\s*(.+)$'),('admitted_memory',r'^\s*Cluster Memory Admitted:\s*(.+)$')]:
    v=re.search(pattern,s,re.M);out[key]=v.group(1) if v else ''
   results.append(out)
  if not results:raise ValueError('No run manifests found')
  with (self.root/'summary.csv').open('w',newline='') as f:
   w=csv.DictWriter(f,fieldnames=list(results[0]));w.writeheader();w.writerows(results)
  print(self.root/'summary.csv')

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--config',default='config.json');p.add_argument('--artifacts',default='artifacts')
 sub=p.add_subparsers(dest='action',required=True)
 for x in ('inspect','freeze','quality','summarize'):sub.add_parser(x)
 for x in ('generate','plan','run'):
  q=sub.add_parser(x);q.add_argument('--case',choices=list(CASE_OVERRIDES),required=True);q.add_argument('--repeat',default='1')
 q=sub.add_parser('compare');q.add_argument('--left',required=True);q.add_argument('--right',required=True)
 a=p.parse_args();lab=Lab(a)
 if a.action in ('inspect','freeze','quality','summarize'):getattr(lab,a.action)()
 elif a.action=='compare':lab.compare(a.left,a.right)
 else:getattr(lab,a.action)(a.case,a.repeat)
if __name__=='__main__':
 try:main()
 except Exception as e:print('ERROR:',e,file=sys.stderr);sys.exit(1)
