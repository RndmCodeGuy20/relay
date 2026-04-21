CREATE TABLE IF NOT EXISTS rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 1,
    selector JSONB NOT NULL DEFAULT '{}'::jsonb,
    targets JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT rules_selector_object_check
    CHECK (jsonb_typeof(selector) = 'object'),

    CONSTRAINT rules_targets_array_check
    CHECK (jsonb_typeof(targets) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_rules_enabled_priority_id
    ON rules (enabled, priority DESC, id);

CREATE INDEX IF NOT EXISTS idx_rules_source_event_type_enabled
    ON rules (source, event_type, enabled);
