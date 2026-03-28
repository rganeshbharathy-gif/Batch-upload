--liquibase formatted sql

--changeset batch-upload:009
--comment: Make PERSON_ID the primary key of DIMENSIONS — remove surrogate ID column; PERSON_ID is the natural key for MERGE matching

-- Drop the existing surrogate PK
ALTER TABLE DIMENSIONS DROP COLUMN ID;

-- Make PERSON_ID NOT NULL and the primary key
ALTER TABLE DIMENSIONS MODIFY PERSON_ID NOT NULL;

ALTER TABLE DIMENSIONS ADD CONSTRAINT PK_DIMENSIONS PRIMARY KEY (PERSON_ID);

-- The composite index on (GRID_ID, PERSON_ID) is no longer needed for MERGE matching;
-- PERSON_ID is now the PK index. Drop it and keep a standalone GRID_ID index for queries.
DROP INDEX IDX_DIM_GRID_PERSON;

CREATE INDEX IDX_DIM_GRID_ID ON DIMENSIONS (GRID_ID);
--rollback DROP INDEX IDX_DIM_GRID_ID;
--rollback ALTER TABLE DIMENSIONS DROP CONSTRAINT PK_DIMENSIONS;
--rollback ALTER TABLE DIMENSIONS ADD ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY;
--rollback CREATE INDEX IDX_DIM_GRID_PERSON ON DIMENSIONS (GRID_ID, PERSON_ID);
