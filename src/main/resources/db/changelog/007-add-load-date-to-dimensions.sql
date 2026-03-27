--liquibase formatted sql

--changeset batch-upload:007
--comment: Add LOAD_DATE to DIMENSIONS to mark which daily load each row belongs to; used by MERGE strategy to identify and delete stale rows
ALTER TABLE DIMENSIONS ADD LOAD_DATE DATE DEFAULT TRUNC(SYSDATE) NOT NULL;

CREATE INDEX IDX_DIM_LOAD_DATE ON DIMENSIONS (LOAD_DATE);
--rollback DROP INDEX IDX_DIM_LOAD_DATE;
--rollback ALTER TABLE DIMENSIONS DROP COLUMN LOAD_DATE;
