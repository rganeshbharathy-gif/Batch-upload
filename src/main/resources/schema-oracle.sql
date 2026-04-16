-- =============================================================================
-- Schema for the Batch-Upload Dimension Loader
-- Run this ONCE against the target Oracle database before the first job launch.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSIONS table
--
-- File-column → table-column mapping:
--   GRID_ID         → grid_id
--   CSI_ID          → person_id       (primary key; merge target)
--   CTY_OF_CTZN_CD  → country_code
--   ECON_SEC_CD     → eco_sector_code
--                     loaded_at       (set to job start time on every merge)
-- -----------------------------------------------------------------------------
CREATE TABLE dimensions (
    person_id        VARCHAR2(100)  NOT NULL,
    grid_id          VARCHAR2(100),
    country_code     VARCHAR2(10),
    eco_sector_code  NUMBER(19),
    loaded_at        TIMESTAMP      NOT NULL,
    CONSTRAINT pk_dimensions PRIMARY KEY (person_id)
);

CREATE INDEX idx_dim_country_sector
    ON dimensions (country_code, eco_sector_code);

-- -----------------------------------------------------------------------------
-- Spring Batch 6 metadata tables (Oracle dialect)
-- Only needed if spring.batch.jdbc.initialize-schema=never (production default).
-- Canonical source: spring-batch-core JAR → org/springframework/batch/core/schema-oracle.sql
-- -----------------------------------------------------------------------------

CREATE SEQUENCE BATCH_STEP_EXECUTION_SEQ START WITH 1 MINVALUE 1 MAXVALUE 9223372036854775807 NOCYCLE;
CREATE SEQUENCE BATCH_JOB_EXECUTION_SEQ  START WITH 1 MINVALUE 1 MAXVALUE 9223372036854775807 NOCYCLE;
CREATE SEQUENCE BATCH_JOB_SEQ            START WITH 1 MINVALUE 1 MAXVALUE 9223372036854775807 NOCYCLE;

CREATE TABLE BATCH_JOB_INSTANCE (
    JOB_INSTANCE_ID NUMBER(19,0)  NOT NULL PRIMARY KEY,
    VERSION         NUMBER(19,0),
    JOB_NAME        VARCHAR2(100) NOT NULL,
    JOB_KEY         VARCHAR2(32)  NOT NULL,
    CONSTRAINT JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)
);

CREATE TABLE BATCH_JOB_EXECUTION (
    JOB_EXECUTION_ID           NUMBER(19,0)   NOT NULL PRIMARY KEY,
    VERSION                    NUMBER(19,0),
    JOB_INSTANCE_ID            NUMBER(19,0)   NOT NULL,
    CREATE_TIME                TIMESTAMP      NOT NULL,
    START_TIME                 TIMESTAMP      DEFAULT NULL,
    END_TIME                   TIMESTAMP      DEFAULT NULL,
    STATUS                     VARCHAR2(10),
    EXIT_CODE                  VARCHAR2(2500),
    EXIT_MESSAGE               VARCHAR2(2500),
    LAST_UPDATED               TIMESTAMP,
    CONSTRAINT JOB_INST_EXEC_FK FOREIGN KEY (JOB_INSTANCE_ID)
        REFERENCES BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
);

CREATE TABLE BATCH_JOB_EXECUTION_PARAMS (
    JOB_EXECUTION_ID NUMBER(19,0) NOT NULL,
    PARAMETER_NAME   VARCHAR2(100) NOT NULL,
    PARAMETER_TYPE   VARCHAR2(100) NOT NULL,
    PARAMETER_VALUE  VARCHAR2(2500),
    IDENTIFYING      CHAR(1)       NOT NULL,
    CONSTRAINT JOB_EXEC_PARAMS_FK FOREIGN KEY (JOB_EXECUTION_ID)
        REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);

CREATE TABLE BATCH_STEP_EXECUTION (
    STEP_EXECUTION_ID  NUMBER(19,0)  NOT NULL PRIMARY KEY,
    VERSION            NUMBER(19,0)  NOT NULL,
    STEP_NAME          VARCHAR2(100) NOT NULL,
    JOB_EXECUTION_ID   NUMBER(19,0)  NOT NULL,
    CREATE_TIME        TIMESTAMP     NOT NULL,
    START_TIME         TIMESTAMP     DEFAULT NULL,
    END_TIME           TIMESTAMP     DEFAULT NULL,
    STATUS             VARCHAR2(10),
    COMMIT_COUNT       NUMBER(19,0),
    READ_COUNT         NUMBER(19,0),
    FILTER_COUNT       NUMBER(19,0),
    WRITE_COUNT        NUMBER(19,0),
    READ_SKIP_COUNT    NUMBER(19,0),
    WRITE_SKIP_COUNT   NUMBER(19,0),
    PROCESS_SKIP_COUNT NUMBER(19,0),
    ROLLBACK_COUNT     NUMBER(19,0),
    EXIT_CODE          VARCHAR2(2500),
    EXIT_MESSAGE       VARCHAR2(2500),
    LAST_UPDATED       TIMESTAMP,
    CONSTRAINT JOB_EXEC_STEP_FK FOREIGN KEY (JOB_EXECUTION_ID)
        REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);

CREATE TABLE BATCH_STEP_EXECUTION_CONTEXT (
    STEP_EXECUTION_ID  NUMBER(19,0)   NOT NULL PRIMARY KEY,
    SHORT_CONTEXT      VARCHAR2(2500) NOT NULL,
    SERIALIZED_CONTEXT CLOB,
    CONSTRAINT STEP_EXEC_CTX_FK FOREIGN KEY (STEP_EXECUTION_ID)
        REFERENCES BATCH_STEP_EXECUTION(STEP_EXECUTION_ID)
);

CREATE TABLE BATCH_JOB_EXECUTION_CONTEXT (
    JOB_EXECUTION_ID   NUMBER(19,0)   NOT NULL PRIMARY KEY,
    SHORT_CONTEXT      VARCHAR2(2500) NOT NULL,
    SERIALIZED_CONTEXT CLOB,
    CONSTRAINT JOB_EXEC_CTX_FK FOREIGN KEY (JOB_EXECUTION_ID)
        REFERENCES BATCH_JOB_EXECUTION(JOB_EXECUTION_ID)
);
