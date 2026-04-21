ALTER TABLE dispatch_tasks
ADD COLUMN IF NOT EXISTS claimed_by TEXT,
ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_tasks_event_relay_unique
    ON dispatch_tasks (event_id, relay_id);

CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_processing_lease_expiry
    ON dispatch_tasks (status, lease_expires_at)
    WHERE status = 'processing';
