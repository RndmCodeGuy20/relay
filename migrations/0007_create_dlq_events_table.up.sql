DROP TABLE IF EXISTS dlq_events;

ALTER TABLE dispatch_tasks
ADD COLUMN IF NOT EXISTS dead_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS dead_reason_code TEXT,
ADD COLUMN IF NOT EXISTS dead_error_text TEXT,
ADD COLUMN IF NOT EXISTS dead_context JSONB NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS dead_retry_count INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_dead_at
    ON dispatch_tasks (dead_at DESC)
    WHERE status = 'dead';

CREATE INDEX IF NOT EXISTS idx_dispatch_tasks_dead_reason_code
    ON dispatch_tasks (dead_reason_code)
    WHERE status = 'dead';
