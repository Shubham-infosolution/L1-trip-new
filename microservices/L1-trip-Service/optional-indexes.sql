-- REVIEW existing indexes and table size first. These are OPTIONAL DBA-run changes.
-- Do not apply blindly; index creation on a large table can consume significant resources.
-- Check first: SHOW INDEX FROM tbl_trip;
-- No business columns are required for the basic implementation.

-- Efficient vehicle locking / active-trip lookup and latest-completion lookup.
CREATE INDEX idx_trip_vehicle_state_completion
  ON tbl_trip (vehicle_no, level, status, trip_status, completion_date);

-- Optional generated shard for eight independent minute refreshes.
-- After applying BOTH statements below, set DB_USE_SHARD_COLUMN: true in src/Database.js.
ALTER TABLE tbl_trip ADD COLUMN trip_worker_shard TINYINT UNSIGNED
  GENERATED ALWAYS AS (MOD(CRC32(UPPER(TRIM(vehicle_no))), 8)) STORED;
CREATE INDEX idx_trip_worker_refresh
  ON tbl_trip (trip_worker_shard, level, status, trip_status, id);

-- Inspect data without modifying it:
-- SELECT id, vehicle_no FROM tbl_trip WHERE level=1 AND status=1
--   AND trip_status IN (0,1)
--   AND BINARY vehicle_no <> BINARY UPPER(TRIM(vehicle_no));
