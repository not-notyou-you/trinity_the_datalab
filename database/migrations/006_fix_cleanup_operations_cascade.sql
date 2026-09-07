-- database/migrations/006_fix_cleanup_operations_cascade.sql
--
-- cleanup_operations.dataset_id was created with ON DELETE CASCADE. The final
-- step of a full dataset delete deletes the datasets row, which immediately
-- cascades and deletes the cleanup_operations row that was just marked
-- COMPLETED. Callers polling GET /datasets/{id}/deletion-progress after the
-- delete finishes then get a 404 instead of the completed progress record.
--
-- cleanup_operations should not depend on the dataset row still existing, so
-- drop the FK constraint (dataset_id is kept as a plain column).

ALTER TABLE cleanup_operations
    DROP CONSTRAINT IF EXISTS cleanup_operations_dataset_id_fkey;

INSERT INTO schema_migrations (version, description)
VALUES ('006', 'Drop ON DELETE CASCADE from cleanup_operations.dataset_id so completed deletion-progress rows survive dataset deletion')
ON CONFLICT (version) DO NOTHING;
