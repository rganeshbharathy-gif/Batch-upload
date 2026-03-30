--liquibase formatted sql

--changeset batch-upload:010
--comment: Add FILTER_COUNT to POD_PROCESSING_LOG to track records filtered by processor (returned null)
ALTER TABLE POD_PROCESSING_LOG ADD FILTER_COUNT NUMBER(19,0) DEFAULT 0;
--rollback ALTER TABLE POD_PROCESSING_LOG DROP COLUMN FILTER_COUNT;
