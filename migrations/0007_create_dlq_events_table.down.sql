DROP INDEX IF EXISTS idx_dispatch_tasks_dead_reason_code;
DROP INDEX IF EXISTS idx_dispatch_tasks_dead_at;

DROP INDEX IF EXISTS idx_dlq_events_event_id;
DROP INDEX IF EXISTS idx_dlq_events_failed_at;
DROP TABLE IF EXISTS dlq_events;

ALTER TABLE dispatch_tasks
DROP COLUMN IF EXISTS dead_retry_count,
DROP COLUMN IF EXISTS dead_context,
DROP COLUMN IF EXISTS dead_error_text,
DROP COLUMN IF EXISTS dead_reason_code,
DROP COLUMN IF EXISTS dead_at;
