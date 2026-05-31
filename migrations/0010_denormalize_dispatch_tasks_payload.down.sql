-- Down migration 0010: drop denormalized columns.

DROP INDEX IF EXISTS idx_dispatch_tasks_source_producer_event_id;

ALTER TABLE dispatch_tasks
    DROP COLUMN IF EXISTS payload,
    DROP COLUMN IF EXISTS producer_event_id,
    DROP COLUMN IF EXISTS event_type,
    DROP COLUMN IF EXISTS source;
