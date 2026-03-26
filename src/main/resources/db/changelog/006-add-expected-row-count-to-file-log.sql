--liquibase formatted sql

--changeset batch-upload:006
--comment: Add EXPECTED_ROW_COUNT to FILE_PROCESSING_LOG to compare footer count vs actual rows processed
ALTER TABLE FILE_PROCESSING_LOG ADD EXPECTED_ROW_COUNT NUMBER(19,0);
--rollback ALTER TABLE FILE_PROCESSING_LOG DROP COLUMN EXPECTED_ROW_COUNT;
