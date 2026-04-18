-- 002_create_outbox_indexes.down.sql

DROP INDEX IF EXISTS idx_dispatch_tasks_dead_tasks;
DROP INDEX IF EXISTS idx_dispatch_tasks_event_id_status;
DROP INDEX IF EXISTS idx_dispatch_tasks_status_visible;

DROP INDEX IF EXISTS idx_outbox_events_relayed;
DROP INDEX IF EXISTS idx_outbox_events_source_event;