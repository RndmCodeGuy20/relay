-- Create publication for outbox_events table
DROP PUBLICATION IF EXISTS relay_pub;
CREATE PUBLICATION relay_pub FOR TABLE outbox_events;

-- Create logical replication slot using pgoutput plugin
SELECT pg_create_logical_replication_slot('relay_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'relay_slot'
);
