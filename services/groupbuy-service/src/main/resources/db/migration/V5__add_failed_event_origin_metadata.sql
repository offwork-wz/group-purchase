ALTER TABLE tb_failed_event
    ADD COLUMN original_partition INTEGER,
    ADD COLUMN original_offset BIGINT;

ALTER TABLE tb_failed_event
    ADD CONSTRAINT uq_failed_event_original_record
        UNIQUE (topic, original_partition, original_offset);
