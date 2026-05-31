-- Migration 0008: Update dispatch_tasks unique constraint to include target
-- This supports the new architecture where one event can have multiple targets.

DROP INDEX IF EXISTS idx_dispatch_tasks_event_relay_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_tasks_event_relay_target_unique
    ON dispatch_tasks (event_id, relay_id, target);

-- Index for dispatcher polling: claimable rows by status and visible time
CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_claimable
    ON dispatch_tasks (status, visible_at)
    WHERE status IN ('pending', 'failed');
