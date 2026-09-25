-- 026_live_area_waiting.sql
--
-- live_areas.status menerima 'WAITING' ("Menunggu giliran"): siklus daerah
-- sudah dipicu tapi masih mengantre di belakang siklus daerah lain
-- (etl/live_monitor.py _CYCLE_LOCK). ADITIF: nilai lama tetap sah.

BEGIN;

ALTER TABLE live_areas DROP CONSTRAINT IF EXISTS chk_live_area_status;
ALTER TABLE live_areas ADD CONSTRAINT chk_live_area_status CHECK (
    status IN ('BACKFILLING', 'ACTIVE', 'RUNNING', 'WAITING', 'ERROR', 'DELETED'));

COMMIT;
