-- Down migration 0008: revert unique constraint back to (event_id, relay_id)

DROP INDEX IF EXISTS idx_dispatch_tasks_event_relay_target_unique;
DROP INDEX IF EXISTS idx_dispatch_tasks_claimable;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_tasks_event_relay_unique
    ON dispatch_tasks (event_id, relay_id);
