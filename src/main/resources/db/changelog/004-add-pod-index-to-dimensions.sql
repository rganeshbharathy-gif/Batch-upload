--liquibase formatted sql

--changeset batch-upload:004
--comment: Add POD_INDEX column to DIMENSIONS to track which pod inserted each row
ALTER TABLE DIMENSIONS ADD POD_INDEX NUMBER(5,0);

CREATE INDEX IDX_DIM_POD_INDEX ON DIMENSIONS (POD_INDEX);
--rollback ALTER TABLE DIMENSIONS DROP COLUMN POD_INDEX;
