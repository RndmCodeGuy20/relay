-- Down migration 0009: restore relay_id (nullable) and the old unique index.

DROP INDEX IF EXISTS idx_dispatch_tasks_event_target_unique;

ALTER TABLE dispatch_tasks
    ADD COLUMN IF NOT EXISTS relay_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_tasks_event_relay_target_unique
    ON dispatch_tasks (event_id, relay_id, target);
