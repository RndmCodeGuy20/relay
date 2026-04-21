DROP INDEX IF EXISTS idx_dispatch_tasks_processing_lease_expiry;
DROP INDEX IF EXISTS idx_dispatch_tasks_event_relay_unique;

ALTER TABLE dispatch_tasks
DROP COLUMN IF EXISTS last_attempt_at,
DROP COLUMN IF EXISTS lease_expires_at,
DROP COLUMN IF EXISTS claimed_by;
