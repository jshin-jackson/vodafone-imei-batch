CREATE DATABASE IF NOT EXISTS imei_validation;
-- No IF NOT EXISTS: stop if a previous test already created this target.
CREATE TABLE ${var:OUT_TABLE}
LIKE ipdr_rep.ipdr_report_hour_requests_imei;
