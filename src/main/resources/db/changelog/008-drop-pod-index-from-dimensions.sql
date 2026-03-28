--liquibase formatted sql

--changeset batch-upload:008
--comment: Remove POD_INDEX from DIMENSIONS — metadata belongs in POD_PROCESSING_LOG, not business data
DROP INDEX IDX_DIM_POD_INDEX;

ALTER TABLE DIMENSIONS DROP COLUMN POD_INDEX;
--rollback ALTER TABLE DIMENSIONS ADD POD_INDEX NUMBER(5,0);
--rollback CREATE INDEX IDX_DIM_POD_INDEX ON DIMENSIONS (POD_INDEX);
