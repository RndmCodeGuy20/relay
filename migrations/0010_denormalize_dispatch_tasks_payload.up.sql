-- Migration 0010: denormalize event payload onto dispatch_tasks so the worker
-- is decoupled from outbox_events. Adds (source, event_type, producer_event_id,
-- payload). Indexed on (source, producer_event_id) for ops lookups.

ALTER TABLE dispatch_tasks
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS producer_event_id UUID,
    ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE dispatch_tasks
    ALTER COLUMN source DROP DEFAULT,
    ALTER COLUMN event_type DROP DEFAULT,
    ALTER COLUMN payload DROP DEFAULT;

CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_source_producer_event_id
    ON dispatch_tasks (source, producer_event_id);
