# Impala IMEI Batch: Performance Analysis and Improvement Plan

**Query ID:** `d64de16e2d62eda1:a19c5c9200000000`  
**Environment:** Cloudera Private Cloud Base 7.3.1.700 / Impala `4.4.0.7.3.1.700-158` / Apache Iceberg / HDFS  
**Execution:** 188 executors / `MT_DOP=5` / 940 source-and-sink fragment instances  
**Status:** Customer edition / 9 November 2026

## 1. Executive Summary

The query **read about 95.6 TiB to write 8,607 rows**. It took **5 hours, 24 minutes, and 26.7 seconds** to finish.

The improvement plan has three priorities:

1. **Reduce file-open delays.** The profile shows long waits while preparing files for reading. Check NameNode RPC performance, the HDFS client, and the file handle cache first.
2. **Read less data.** The IMEI filter removes most rows, but the profile shows no page or row-group skipping from the runtime or statistics filters. Test a static IMEI list and a better data layout for IMEI searches.
3. **Use shared resources more efficiently.** Review this query's memory limit and batch concurrency. Reduce the number of small report files.

**The observed bottleneck and its underlying cause are different things.** Long file-open and I/O waits are confirmed. NameNode overload is a leading investigation target, not a confirmed root cause. SQL changes must be tested for both speed and correct results.

### Next Immediate Actions

- Monitor NameNode RPC latency and file handle cache misses during a representative run.
- Capture a low-load baseline on one representative date partition, then test `MEM_LIMIT=8g` separately with the concurrency limit unchanged.
- Compare static IMEI filtering and late materialization on that same date and fixed snapshots. Validate the selected candidate over seven days only after the smaller tests pass.

## 2. Key Measurements

| Item | Result | Meaning |
|---|---:|---|
| Total elapsed time | 5h 24m 26.7s | Most time was spent executing the query |
| Planning | About 84.1s | A secondary improvement area |
| Planning complete to all backends started | About 31.9s | Includes admission and startup |
| Source scan operator time | Average about 4h 20m; maximum about 5h 20m | Main bottleneck |
| Source data read | About 95.6 TiB / 1.75 trillion rows | Very large compared with the result |
| Rows leaving the scan / final output | 66,259 / 8,607 | Row filtering is effective |
| Output size / files created | About 10.96 MiB / 929 files | About 12 KiB per file in this run |
| Local and short-circuit reads | About 98.1% of bytes read | Remote reads are not the main issue |
| Scratch writes | Zero | No evidence of spilling to disk |
| Join strategy | Broadcast | Already used by the original query |
| Metastore update | About 3.35s | Final commit is not the main bottleneck |

**How to read the timings:** Query duration is real elapsed time. Open and read counters add time across parallel I/O threads. In `Averaged Fragment F00`, these totals are then averaged across instances. Do not add them to query duration. F00 completion time averaged about 4h 22m and reached about 5h 22m. The slowest execution path largely determines when the query finishes.

## 3. Bottleneck Analysis

### 3.1 File-open delays: investigate first

The average scan instance reported:

| Counter | Value |
|---|---:|
| `TotalRawHdfsOpenFileTime` | About 12.6h |
| `TotalRawHdfsReadTime` | About 4.4m |
| `ScannerIoWaitTime` | About 4.1h |
| File handle cache misses | About 5,622 |

`TotalRawHdfsReadTime` measures accumulated time inside read operations. It is not a measurement of physical disk activity alone. These results point to **delays in opening files and preparing reads**, rather than read operations alone.

Other evidence supports checking a shared service or client path:

- Across scan instances, the 10th percentile of accumulated open time was about 12.1h and the 90th percentile was about 13.2h. The delay was widespread across instances.
- Among the 929 writer instances that created files, `TmpFileCreateTimer` had a median of about 31.8s and a maximum of about 64.4s. Both opening input files and creating output files were slow.
- The scan instances recorded about 5.28 million file handle cache misses in total. The cache is working, but repeated open costs and handle reuse need investigation.

This makes the NameNode and shared HDFS client paths important investigation targets. It does not prove NameNode overload. Instance-level timing distributions are not host-level distributions. Also, dividing cache misses by file count does not establish the exact number of times each file was reopened.

### 3.2 IMEI filtering does not reduce storage reads enough

Date partition pruning is already working. The source has 972,708 files in total; the selected seven days contain 247,069 files. The plan's `HDFS partitions=1/1` label does not mean date pruning failed.

The IMEI runtime filter removes many rows. However, page and row-group skip counters for runtime and statistics filters are zero. The late-materialization page-skip counter is also zero. **The query reads a large volume before discarding unwanted rows.**

The `OR` condition alone does not explain this behavior. The request-dependent conditions and the distribution of IMEI values inside files also matter. The current ordering properties do not include `imei_14`, but properties alone do not prove the actual sort order or that IMEIs are randomly spread across files.

### 3.3 Memory admission and concurrent queries

`MEM_LIMIT` is 35 GiB per node. The profile reports `Cluster Memory Admitted: 6615 GB`. This is an admission-accounting value; it does not mean that much physical memory was actually used. F00 peak memory was about 2 GiB per host, with no spilling.

**A lower memory limit for this query is an early improvement candidate.** Start by testing 8 GiB. Consider 6 GiB only after validation. The 2 GiB observation is for F00, so also check the full query's per-host memory, minimum reservations, pool limits, and clamp policy. Confirm the effective limit and admission-accounting value in the new profile.

This change targets memory admission efficiency. It does not directly fix file-open delays or the long scan. **Keep the batch concurrency limit unchanged during the first comparison.** If a lower limit admits more large scans at once, HDFS contention may increase. There were 17 running queries in the executor group when this query was admitted, but their workload types are unknown.

## 4. Recommended Actions

### 4.1 Investigate HDFS file-open delays

| Area | Check | Decision basis |
|---|---|---|
| NameNode | RPC queue and processing latency, request rate, GC | Compare with the query's slow period |
| Executor HDFS client | JVM thread dumps, GC, connection and authentication logs | Identify where threads are waiting |
| File handle cache | Limit, occupancy, hits, misses, evictions | Increase capacity only if evidence supports it |
| Concurrent workload | Large scans and HDFS traffic | Compare with the same query under low load |

In Cloudera Manager, check **Maximum Cached File Handles (`max_cached_file_handles`)**. Cache hits already exist, so the cache is not simply disabled. Check occupancy, evictions, and NameNode load before increasing the limit.

There is no confirmed evidence that compaction ran alongside this query. A catalog-version change during planning does not establish concurrent compaction.

### 4.2 SQL change: add a static IMEI filter and preserve the original logic

**Improve the SQL alongside the infrastructure.** Add a complete static IMEI list while keeping the output columns, join, date filter, and time OR condition unchanged.

Attachment B reports 115 distinct IMEIs, but its detailed list contains only 114. Generate the list from a fixed request snapshot; do not guess the missing value.

#### 4.2.1 Complete original production SQL: reference only

This is the original statement from the profile. It writes to the production table. **Do not run it as a benchmark.** Use the test baseline below.

```sql
insert into ipdr_rep.ipdr_report_hour_requests_imei
SELECT
    a.imsi,
    a.imei,
    a.imei_14,
    a.msisdn,
    a.mac_address,
     a.source_ip,
    a.source_port,
     a.destination_ip,
     a.destination_port,
     a.translated_ip,
     a.translated_port,
     a.first_cell_id_name_location,
    a.session_start_date_time,
     a.session_end_date_time,
    cast(a.duration_in_sec as string),
     a.data_volume_uplink,
    a.data_volume_downlink,
    a.pgw_ip_address,
    a.charging_id,
    a.access_point_name,
     a.pdp_address_ipv4,
    a.pdp_address_ipv6,
     a.cgi_id,
    a.rat,
    a.pdp_type,
    'N' as tethering,
     'N' as application_name,
    'NA' as ipdr_filename,
    'NA' cgf_filename,
    'NA' cgnat_filename,
    a.cgi_id as roaming_nw_ceid,
     '' as first_cellid_name_address,
     'NA' as subs_circle,
    'NA' as subs_roam_circle,
    a.date_partition,
     b.incident_number,
    b.req_id
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI b
INNER JOIN ipdr_db.ipdr_cisco_oct_2026 a
    ON a.imei_14 = b.report_value
WHERE (a.date_partition IN ('2026-10-17', '2026-10-16', '2026-10-15', '2026-10-14', '2026-10-13', '2026-10-12', '2026-10-11'))
  AND b.num_type = 'IMEI'
  AND (session_start_date_time between b.from_date and b.to_date  OR session_end_date_time between b.from_date and b.to_date);
```

#### 4.2.2 Pin the inputs and prepare test tables

The test SQL below uses the exact snapshots recorded in the original profile:

- Request snapshot: `4865977338790686061`
- Source snapshot: `5180117802424828212`

Check that both snapshots are still available:

```sql
SELECT snapshot_id, committed_at
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI.snapshots
WHERE snapshot_id = 4865977338790686061;

SELECT snapshot_id, committed_at
FROM ipdr_db.ipdr_cisco_oct_2026.snapshots
WHERE snapshot_id = 5180117802424828212;
```

If a snapshot has expired, select a new, agreed source/request pair and replace its ID in **every** test statement. Use the main/current snapshot information in `DESCRIBE FORMATTED` if testing the current table state; do not assume the newest committed snapshot is always the current one. **The source and request table owners must keep the selected snapshots available until testing and result validation are complete.** They should exclude those snapshots from scheduled or manual expiration, or pause the relevant expiration job for the test period if selective protection is not available. Apply this only to the affected tables and restore the normal retention process after sign-off. Retention settings alone do not prove that a selected old snapshot is protected.

Prepare two separate Iceberg test tables, each with the same 37 output columns, types, and column order as the production report table:

- `ipdr_rep.ipdr_report_imei_baseline_test`
- `ipdr_rep.ipdr_report_imei_static_test`

Use a fresh empty output table for every test case, or save each result before a controlled reset. The two names above are examples; use separate names for all four matrix cases when retaining their outputs. Repeated INSERTs append rows and would invalidate the comparison. No test statement below writes to the production report table.

**Start with one date partition.** The test examples below use `2026-10-11`. Confirm that this date contains matching IMEI results; otherwise choose another representative date within the original seven-day window. Use exactly the same date in every comparison. Retain the full request snapshot and the original time OR condition; do not prefilter requests by the chosen date.

#### 4.2.3 Complete baseline SQL with fixed inputs

This one-day baseline preserves the output, join, and time logic. It changes the output target, fixes both input snapshots, and narrows the date filter for an initial low-cost comparison. It is not the full seven-day production baseline.

```sql
INSERT INTO ipdr_rep.ipdr_report_imei_baseline_test
SELECT
    a.imsi,
    a.imei,
    a.imei_14,
    a.msisdn,
    a.mac_address,
     a.source_ip,
    a.source_port,
     a.destination_ip,
     a.destination_port,
     a.translated_ip,
     a.translated_port,
     a.first_cell_id_name_location,
    a.session_start_date_time,
     a.session_end_date_time,
    cast(a.duration_in_sec as string),
     a.data_volume_uplink,
    a.data_volume_downlink,
    a.pgw_ip_address,
    a.charging_id,
    a.access_point_name,
     a.pdp_address_ipv4,
    a.pdp_address_ipv6,
     a.cgi_id,
    a.rat,
    a.pdp_type,
    'N' as tethering,
     'N' as application_name,
    'NA' as ipdr_filename,
    'NA' cgf_filename,
    'NA' cgnat_filename,
    a.cgi_id as roaming_nw_ceid,
     '' as first_cellid_name_address,
     'NA' as subs_circle,
    'NA' as subs_roam_circle,
    a.date_partition,
     b.incident_number,
    b.req_id
FROM (
    SELECT * FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
    FOR SYSTEM_VERSION AS OF 4865977338790686061
) b
INNER JOIN (
    SELECT * FROM ipdr_db.ipdr_cisco_oct_2026
    FOR SYSTEM_VERSION AS OF 5180117802424828212
) a
    ON a.imei_14 = b.report_value
WHERE a.date_partition = DATE '2026-10-11'
  AND b.num_type = 'IMEI'
  AND (session_start_date_time between b.from_date and b.to_date  OR session_end_date_time between b.from_date and b.to_date);
```

#### 4.2.4 Generate the complete modified test SQL

The following SELECT returns a complete one-day INSERT with the same fixed inputs and a static IMEI list. It does not execute the INSERT. Double quotes enclose the generated SQL text, avoiding nested single-quote escaping.

```sql
WITH imei_values AS (
  SELECT DISTINCT report_value
  FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
  FOR SYSTEM_VERSION AS OF 4865977338790686061
  WHERE num_type = 'IMEI' AND report_value IS NOT NULL
), imei_list AS (
  SELECT
    COUNT(*) AS imei_count,
    SUM(CASE WHEN report_value RLIKE '^[0-9]+$' THEN 0 ELSE 1 END)
      AS invalid_imei_count,
    GROUP_CONCAT(CONCAT(CHR(39), report_value, CHR(39)), ', ')
      AS imei_in_list,
    SUM(LENGTH(report_value) + 2) + 2 * (COUNT(*) - 1)
      AS expected_list_bytes
  FROM imei_values
)
SELECT
  imei_count,
  invalid_imei_count,
  LENGTH(imei_in_list) AS actual_list_bytes,
  expected_list_bytes,
  CONCAT(
    "INSERT INTO ipdr_rep.ipdr_report_imei_static_test
SELECT
    a.imsi,
    a.imei,
    a.imei_14,
    a.msisdn,
    a.mac_address,
     a.source_ip,
    a.source_port,
     a.destination_ip,
     a.destination_port,
     a.translated_ip,
     a.translated_port,
     a.first_cell_id_name_location,
    a.session_start_date_time,
     a.session_end_date_time,
    cast(a.duration_in_sec as string),
     a.data_volume_uplink,
    a.data_volume_downlink,
    a.pgw_ip_address,
    a.charging_id,
    a.access_point_name,
     a.pdp_address_ipv4,
    a.pdp_address_ipv6,
     a.cgi_id,
    a.rat,
    a.pdp_type,
    'N' as tethering,
     'N' as application_name,
    'NA' as ipdr_filename,
    'NA' cgf_filename,
    'NA' cgnat_filename,
    a.cgi_id as roaming_nw_ceid,
     '' as first_cellid_name_address,
     'NA' as subs_circle,
    'NA' as subs_roam_circle,
    a.date_partition,
     b.incident_number,
    b.req_id
FROM (
    SELECT * FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
    FOR SYSTEM_VERSION AS OF 4865977338790686061
) b
INNER JOIN (
    SELECT * FROM ipdr_db.ipdr_cisco_oct_2026
    FOR SYSTEM_VERSION AS OF 5180117802424828212
) a
    ON a.imei_14 = b.report_value
WHERE a.date_partition = DATE '2026-10-11'
  AND b.num_type = 'IMEI'
  AND (session_start_date_time between b.from_date and b.to_date  OR session_end_date_time between b.from_date and b.to_date)
  AND a.imei_14 IN (",
    imei_in_list,
    ");"
  ) AS optimized_sql
FROM imei_list;
```

Before running `optimized_sql`:

1. Confirm `imei_count` is greater than zero and matches the distinct non-NULL IMEIs in the selected request snapshot. Do not assume the count must be 115 if a different snapshot is used.
2. Confirm `invalid_imei_count=0`. The generator is designed for digit-only IMEIs. If other characters occur, stop and add validated literal escaping; do not remove those requests silently.
3. Confirm `actual_list_bytes=expected_list_bytes`. For 115 values of 14 digits, the quoted list with comma-space separators is **2,068 bytes**.
4. Export the full generated string and check that the client has not truncated it. The two length values check the list inside Impala, not the completeness of the client export.
5. Confirm the generated target is `ipdr_rep.ipdr_report_imei_static_test`, then run EXPLAIN and execute the test.

**List export:** Validate the counts and lengths first. Then copy or export only the `optimized_sql` cell as plain text to a `.sql` file. Do not save the whole result row: it also contains four validation fields. Remove any client-added headers or CSV/TSV wrapping, and check the complete statement in a SQL editor before EXPLAIN. The file must start with INSERT and end with `);`. If the client cannot export a multiline cell reliably, use the single-line IN-list workflow below. Check the exported list against its server-side count and length. The expected list is only 2,068 bytes for 115 IMEIs of 14 digits.

For a simpler manual workflow, generate only the list from the same fixed snapshot:

```sql
SELECT GROUP_CONCAT(CONCAT(CHR(39), report_value, CHR(39)), ', ')
  AS imei_in_list
FROM (
  SELECT DISTINCT report_value
  FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI
  FOR SYSTEM_VERSION AS OF 4865977338790686061
  WHERE num_type = 'IMEI' AND report_value IS NOT NULL
) t;
```

Append `AND a.imei_14 IN (<imei_in_list>)` to the complete test baseline, replacing the placeholder with the returned literals and changing only the output target to the static-test table. Keep both snapshot IDs unchanged.

#### 4.2.5 Validate the change

- Within each one-day comparison, the static IMEI condition is the only filter change between baseline and candidate. Keep the same one-day date condition and original time OR condition in both. For final validation, restore the original seven-day date IN in both statements.
- Only the IN list uses distinct IMEIs. Keep the request join so separate `req_id` values are preserved.
- Do not add request-date prefilters, extra time AND conditions, sorting, or join hints to this comparison.
- Compare total rows, rows per `req_id`, and duplicate counts across the complete output.
- Measure bytes read, elapsed time, CPU, and file/page/row-group skip counters. A static list may provide little benefit if file value ranges are broad. A zero or unavailable `Files rejected` counter alone is not a complete measure of success.

For the regular production workflow, update statistics after loading the small request table:

```sql
COMPUTE STATS ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI;
```

### 4.3 Session options: test one change at a time

```sql
-- Check current options and support.
SET ALL;

-- Test A: lower the memory limit for this query only.
SET MEM_LIMIT=8g;

-- Test B: limit writers if supported; test separately from Test A.
SET MAX_FS_WRITERS=8;
```

| Option | Test approach |
|---|---|
| `MEM_LIMIT=8g` | A starting value, not a final recommendation. Check admission, the effective limit, out-of-memory errors, and spilling |
| `MAX_FS_WRITERS=8` | A starting value. Check writer and scan fragment host/instance counts in EXPLAIN |
| `MT_DOP=2` versus `5` | Compare scan parallelism and I/O contention under similar load |
| Late materialization | Use SET ALL to check `PARQUET_LATE_MATERIALIZATION_THRESHOLD`. Cloudera describes the behavior before 7.3.2 as disabled by default (`-1`). If the effective value is `-1`, test `20` with and without the static IN list |

`MAX_FS_WRITERS` limits writer instances, not the final number of files. Partitioning and file size can affect the result. Confirm that it changes the Iceberg INSERT plan in the installed build. If it does not, first materialize the small matched result in an isolated staging table. Then test a final INSERT from that small table on one node. Verify the final plan and writer count before use; keep the large source scan distributed.

Late materialization avoids unnecessary decoding and creation of column values. It does not guarantee fewer file opens or fewer physical bytes read. Cloudera's 7.3.2 release notes describe the previous default as `-1` (disabled), so checking the effective value is important for this 7.3.1.700 installation. The release notes do not replace checking the actual session and build fixes.

If the current value is `-1`, use the four one-day cases in Section 7.1. Each original-filter case keeps the selected one-day date and original time OR condition. Reuse those runs as the baseline and static-IN tests rather than executing additional copies. Keep memory and writer tests separate from this matrix.

```sql
-- Run only after checking the effective value and build support.
SET PARQUET_LATE_MATERIALIZATION_THRESHOLD=20;
```

If already enabled, use its current value for the first comparison and monitor the counters. A zero skip counter does not, by itself, prove the feature was disabled.

## 5. Data Layout and Report Files

### 5.1 Source: test the search layout before full-table compaction

The average source file is about 382 MiB. Only about 0.087% of files are smaller than 64 MiB, and the referenced snapshot has zero delete files. A daily partition with 42,990 files is not, by itself, proof of a small-files problem.

| Option | Potential benefit | Conditions and limits |
|---|---|---|
| Sort or cluster by IMEI within date partitions | Narrow IMEI ranges and skip more files or row groups | Test one day's data first; compare rewrite costs and other query types |
| Add a Parquet Bloom filter on `imei_14` to new files | Skip row groups that cannot contain the requested values | Check writer/reader support, predicates, distinct-value counts, filter size, and dictionary encoding; files may still need to be opened |
| Maintain a daily IMEI-to-MSISDN mapping | Add a useful MSISDN filter using the existing layout | Keep all valid mappings, handle updates and NULLs, and retain the original IMEI condition; building historical mappings also requires reading data |

A Bloom filter stored in a Parquet file is different from a runtime Bloom filter built during a query. Adding filters to new files does not update existing files.

Do not use the mapping as a required filter until its completeness and scan benefit are proven. These alternatives can leave the original sort order unchanged, but they still add storage, write, maintenance, or join costs. Do not promise zero impact on other query types.

### 5.2 Report: reduce writers and clean up small files

The full report table contains about 87.14 MiB in 5,563 files, averaging about 16 KiB per file. This execution created 929 files averaging about 12 KiB. These are different measurements.

- Use fewer final writers if `MAX_FS_WRITERS` is supported and reflected in the plan.
- Consider compaction of existing report files during a maintenance window.
- Do not assume `SHUFFLE` or a final `ORDER BY` will reduce writer or file counts. They can add sorting and exchange costs.
- If writer limiting is not reflected in the plan, use an isolated staging table for the small extracted result and test a one-node final INSERT from it. Check that final plan before use.
- Do not reduce the large source scan itself to a single node.

## 6. Changes Not Recommended Without Further Evidence

| Change or claim | Reason |
|---|---|
| Keep only requests overlapping the partition-date range | May lose results unless partition dates are proven to match session times |
| Add AND bounds to both session start and end | May change NULL handling or exclude rows accepted by the original OR condition |
| Force pushdown with a CTE and CROSS JOIN | Values calculated at runtime are not guaranteed to become planning-time constants |
| Deduplicate the request table by IMEI | May remove results for separate requests |
| Replace OR with a simple UNION ALL | May duplicate results and scan the source twice |
| Force Broadcast to eliminate a large shuffle | The original query already uses Broadcast |
| Describe all 17 concurrent queries as heavy | Their workload types were not supplied |
| Promise 10–15 minutes, a 95% reduction, or no more queuing | No benchmark supports these claims |

## 7. Rollout and Validation

### 7.1 Start with a one-day comparison

A full-window run originally read about 95.6 TiB and took more than five hours. Do not repeat that workload for every option. Start with one representative date containing matched results, using the same source and request snapshots in every test. A one-day runtime is not guaranteed to be exactly one-seventh of the full-window runtime.

If late materialization is currently disabled (`-1`), compare these four cases:

| Case | Filters | Late-materialization setting |
|---|---|---|
| A: baseline | Selected date + original join and time conditions | `-1` |
| B | Same as A + static IMEI IN | `-1` |
| C | Same as A | `20` |
| D | Same as A + static IMEI IN | `20` |

Keep MEM_LIMIT, MT_DOP, writer settings, date, and load conditions unchanged across the matrix. Record the effective options in every profile. Case A is the baseline and Case B is the static-IN comparison; do not run extra copies solely to satisfy separate checklist items. If late materialization is already enabled, begin with the existing setting and compare original filters against static IN. Do not disable it just to create four test cases.

Use a separate output table for each retained case. Compare full output values and duplicate counts, as well as total rows and rows per `req_id`. Initial tests screen the options. Repeat a case only when needed to separate a real improvement from cache or workload variation.

### 7.2 Final seven-day validation

After the one-day tests pass, select the best candidate and restore the original date condition in the candidate and any new baseline:

```sql
a.date_partition IN (
  '2026-10-17', '2026-10-16', '2026-10-15', '2026-10-14',
  '2026-10-13', '2026-10-12', '2026-10-11'
)
```

Run the selected candidate over the full seven-day window. If a complete baseline output from the same snapshots is saved, compare against it and avoid another full baseline run. The original profile's count of 8,607 rows alone is not enough to prove equal results. Do not assume the current production report table is an isolated baseline; it may contain other batches or later writes.

If no suitable baseline output exists, run a full-window baseline as well. A one-day test cannot prove seven-day result equality or predict the full-window speedup. Keep the selected snapshots protected until this validation is complete.

### 7.3 Action plan

| Stage | Action | Success measure |
|---|---|---|
| Baseline | Check options and pools, prepare test tables, protect snapshots, capture one-day results | A reproducible baseline and comparable output |
| First | Investigate NameNode/client/cache delays; test 8 GiB memory separately | Better evidence for open delays; lower admission accounting without errors or spills |
| Second | Run the applicable one-day filter/late-materialization comparisons | Equal results with lower reading, CPU work, or runtime |
| Third | Test writer limits, MT_DOP, and batch concurrency separately | Fewer output files and better batch throughput |
| Final check | Run the selected seven-day candidate; reuse a saved full baseline or run one if needed | Correct full-window results and measured end-to-end improvement |
| Medium term | Compare IMEI layout, Bloom filters, and mappings | Lower repeated search cost after write and maintenance costs |

Change one factor at a time. Record cache state and concurrent load. Use the real INSERT with all required output columns for the final benchmark; a replacement COUNT query can read different columns and use a different plan.

**Track:** total elapsed time, bytes read, open and I/O waits, CPU, per-host peak memory and spills, admission wait and memory accounting, output file count, and batches completed per hour.

## Appendix: Evidence and References

### Source material

- Original query profile: `Profile_Query ID d64de16e2d62eda1a19c5c9200000000.txt`. Execution summary: lines 121–130; timeline: lines 164–178; average F00: line 19518 onward. Individual instance counters were also examined.
- Attachments A–D: table definitions, request data, source files and snapshots, report statistics, and metadata schemas.

### Official references

- [Cloudera 7.3.1: Scalability and file handle caching](https://docs.cloudera.com/runtime/7.3.1/impala-reference/topics/impala-scalability.html)
- [Cloudera 7.3.1: Performance, Parquet Bloom filters, and late materialization](https://docs.cloudera.com/cdp-private-cloud-base/7.3.1/impala-reference/impala-reference.pdf)
- [Apache Impala IMPALA-8125: Limit HDFS writer instances](https://issues.apache.org/jira/browse/IMPALA-8125). Verify Iceberg INSERT behavior in the installed build using SET and EXPLAIN.
- [Apache Impala: GROUP_CONCAT](https://impala.apache.org/docs/build/asf-site-html/topics/impala_group_concat.html)
- [Apache Impala: String literals](https://impala.apache.org/docs/build/html/topics/impala_literals.html)
- [Apache Impala: Iceberg snapshot queries](https://impala.apache.org/docs/build/html/topics/impala_iceberg.html)
- [Apache Impala: Join and INSERT hints](https://impala.apache.org/docs/build/html/topics/impala_hints.html)
- [Apache Iceberg 1.4.0: Rewrite and sort procedures](https://iceberg.apache.org/docs/1.4.0/spark-procedures/)
- [Cloudera: Late-materialization behavior changes](https://docs.cloudera.com/cdp-private-cloud-base/latest/private-release-notes/topics/rt-behavioral-changes-impala.html). Cloudera describes the default before 7.3.2 as `-1` (disabled). Confirm the effective setting with SET ALL.

This report is based on the supplied files and official documentation. No database settings were changed and no queries were rerun during this review.

