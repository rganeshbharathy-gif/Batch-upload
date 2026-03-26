--liquibase formatted sql

--changeset batch-upload:005
--comment: Add EXPECTED_ROW_COUNT to POD_PROCESSING_LOG for footer row count validation
ALTER TABLE POD_PROCESSING_LOG ADD EXPECTED_ROW_COUNT NUMBER(19,0);
--rollback ALTER TABLE POD_PROCESSING_LOG DROP COLUMN EXPECTED_ROW_COUNT;
