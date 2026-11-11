#!/usr/bin/env python3
"""Extract useful lines without summing averaged and instance counters together."""
import pathlib,re,sys
keys=['Duration:','Query Options','Query ID','Query (id=','Admission result:', 'Initial admission queue reason:',
'Cluster Memory Admitted:', 'TotalBytesRead:', 'TotalCpuTime:', 'FilesCreated:', 'NumModifiedRows:',
'PerHostPeakMemUsage:', 'PeakMemoryUsage:', 'ScratchBytesWritten:', 'TotalRawHdfsOpenFileTime:',
'TotalRawHdfsReadTime:', 'ScannerIoWaitTime:', 'CachedFileHandlesHitCount:', 'CachedFileHandlesMissCount:',
'NumRuntimeFilteredPages:', 'NumRuntimeFilteredRowGroups:', 'NumStatsFilteredPages:', 'NumStatsFilteredRowGroups:',
'NumPagesSkippedByLateMaterialization:', 'TmpFileCreateTimer:', 'NumBackends:', 'BytesReadLocal:',
'BytesReadShortCircuit:', 'Averaged Fragment', 'HDFS WRITER', 'HDFS_SCAN_NODE','NumRowGroups:','RowsRead:']
for name in sys.argv[1:]:
 p=pathlib.Path(name);text=p.read_text();out=[];in_average=False;instances=False
 for n,l in enumerate(text.splitlines(),1):
  if re.match(r'^    Averaged Fragment ',l):in_average=True;instances=False
  elif re.match(r'^    Fragment F',l):in_average=False;instances=True
  if (not instances) and any(k in l for k in keys):out.append(f'{n}: {l}')
 target=p.with_suffix('.fields.txt');target.write_text('\n'.join(out)+'\n');print(target)
