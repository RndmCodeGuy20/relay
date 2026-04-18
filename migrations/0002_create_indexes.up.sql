-- 002_create_indexes.up.sql

-- Outbox events indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_events_source_event
    ON outbox_events (source, event_id);

CREATE INDEX IF NOT EXISTS idx_outbox_events_relayed
    ON outbox_events (relayed)
    WHERE relayed = FALSE;

-- Dispatch tasks indexes
CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_status_visible
    ON dispatch_tasks (status, visible_at)
    WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_event_id_status
    ON dispatch_tasks (event_id, status);

CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_dead_tasks
    ON dispatch_tasks (status)
    WHERE status = 'dead';