import collections,importlib.util,json,pathlib,sqlite3,tempfile,unittest
from types import SimpleNamespace
from validate import Lab,OUTPUT,CASE_OVERRIDES
class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  base=pathlib.Path(self.tmp.name)
  c=json.loads(pathlib.Path('config.example.json').read_text());c['run_id']='test_run'
  self.cp=base/'config.json';self.cp.write_text(json.dumps(c))
  self.lab=Lab(SimpleNamespace(config=str(self.cp),artifacts=str(base/'artifacts')))
  import hashlib
  f={k:c[k] for k in ('source_table','request_table','test_date','full_dates')};f.update(source_snapshot=123,request_snapshot=456,imeis=['00000000000001','00000000000002'])
  f['imei_sha256']=hashlib.sha256(json.dumps(f['imeis'],ensure_ascii=False).encode()).hexdigest()
  (self.lab.root/'frozen.json').write_text(json.dumps(f));self.f=f
 def test_generation(self):
  for case in CASE_OVERRIDES:
   stem,target=self.lab.generate(case,'1');s=(self.lab.root/(stem+'_run.sql')).read_text()
   self.assertIn('FOR SYSTEM_VERSION AS OF 123',s);self.assertIn('FOR SYSTEM_VERSION AS OF 456',s)
   self.assertIn("a.session_start_date_time BETWEEN b.from_date AND b.to_date",s)
   self.assertNotIn('TRUNCATE',s);self.assertNotIn('DROP ',s)
   self.assertIn('SUMMARY;\nPROFILE;',s)
  a=self.lab.select(self.f,False);b=self.lab.select(self.f,True)
  self.assertTrue(b.startswith(a));self.assertIn("'00000000000001'",b)
 def test_multiset_detects_equal_counts_but_different_duplicates_and_nulls(self):
  db=sqlite3.connect(':memory:')
  names=', '.join(n+' TEXT' for n,_ in OUTPUT)
  for t in ('a','b'):db.execute(f'CREATE TABLE {t} ({names})')
  x=[None]*37;x[0]='x';y=[None]*37;y[0]='y'
  q='('+','.join('?' for _ in OUTPUT)+')'
  for row in (x,x,y):db.execute('INSERT INTO a VALUES '+q,row)
  for row in (x,y,y):db.execute('INSERT INTO b VALUES '+q,row)
  cols=', '.join(n for n,_ in OUTPUT)
  sql=f'WITH c AS (SELECT {cols},1 AS weight FROM a UNION ALL SELECT {cols},-1 AS weight FROM b),d AS (SELECT {cols},SUM(weight) delta FROM c GROUP BY {cols} HAVING SUM(weight)<>0) SELECT COUNT(*),SUM(ABS(delta)) FROM d'
  self.assertEqual(db.execute(sql).fetchone(),(2,2))
 def test_stale_input_rejected(self):
  self.lab.c['test_date']='2026-10-12'
  with self.assertRaises(ValueError):self.lab.frozen()
 def test_fixture_logic(self):
  # Independently check endpoint OR semantics, NULLs, duplicates, and cross-date requests.
  requests=[('k1','r1','11 10','11 11'),('k1','r2','11 10','11 11'),('k1','r3','13 09','13 11'),('k2','r4','11 10','11 11')]
  src=[('inside','k1','11 10:15','11 10:30')]*2+[('boundary','k1','11 09','11 10'),('covers','k1','11 09','11 12'),('null_end','k1','11 10:20',None),('null_start','k1',None,'11 10:20'),('outside','k1','11 08','11 09'),('cross_date','k1','11 23','13 10'),('leading_zero','k2','11 10:20','11 10:30')]
  def between(v,lo,hi):
   # Normalize abbreviated fixture times for this independent check.
   def norm(v):return v if ':' in v else v+':00'
   return v is not None and norm(lo)<=norm(v)<=norm(hi)
  result=[(row[0],r[1]) for row in src for r in requests if row[1]==r[0] and (between(row[2],r[2],r[3]) or between(row[3],r[2],r[3]))]
  self.assertEqual(len(result),12);self.assertEqual(collections.Counter(x[0] for x in result)['cross_date'],1)
if __name__=='__main__':unittest.main()
