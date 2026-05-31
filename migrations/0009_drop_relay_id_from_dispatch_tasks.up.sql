-- Migration 0009: drop relay_id from dispatch_tasks.
-- The fan-out distinguisher is `target`; `relay_id` carried no independent
-- information and made dedup keys non-deterministic across replay.

DROP INDEX IF EXISTS idx_dispatch_tasks_event_relay_target_unique;

ALTER TABLE dispatch_tasks
    DROP COLUMN IF EXISTS relay_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_tasks_event_target_unique
    ON dispatch_tasks (event_id, target);
