SELECT VERSION();
SET ALL;
SELECT snapshot_id, committed_at
FROM ipdr_db.ipdr_cisco_oct_2026.snapshots
ORDER BY committed_at DESC LIMIT 10;
SELECT snapshot_id, committed_at
FROM ipdr_rep.LGL_CGNAT_REQUEST_SPLIT_IMEI.snapshots
ORDER BY committed_at DESC LIMIT 10;
