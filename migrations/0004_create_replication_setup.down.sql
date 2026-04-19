-- Drop replication slot
SELECT pg_drop_replication_slot('relay_slot')
WHERE EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'relay_slot'
);

-- Drop publication
DROP PUBLICATION IF EXISTS relay_pub;
