-- =============================================================================
-- Oracle DDL for the DIMENSIONS table and Spring Batch metadata tables.
-- Run this script ONCE against your Oracle schema before the first job launch.
-- =============================================================================

-- ── DIMENSIONS table ─────────────────────────────────────────────────────────

CREATE TABLE dimensions (
    id            NUMBER         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    csi_id        VARCHAR2(100)  NOT NULL,
    person_id     VARCHAR2(100)  NOT NULL,
    country_code  VARCHAR2(10)   NOT NULL,
    economic_code VARCHAR2(50)   NOT NULL,
    created_at    TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL
);

-- Supporting indexes – add / remove based on your query patterns.
CREATE INDEX idx_dim_csi_id       ON dimensions (csi_id);
CREATE INDEX idx_dim_person_id    ON dimensions (person_id);
CREATE INDEX idx_dim_country_code ON dimensions (country_code);

-- Performance hint for bulk loads: nologging reduces redo log writes.
-- Re-enable LOGGING and rebuild indexes after the load if the table is in
-- a FORCE LOGGING tablespace.
-- ALTER TABLE dimensions NOLOGGING;


-- ── Spring Batch metadata tables (Oracle dialect) ─────────────────────────
-- Source: spring-batch-core jar → org/springframework/batch/core/schema-oracle10g.sql
-- Only needed once per schema. Spring Boot sets initialize-schema=never so
-- Boot will NOT auto-create these; run them manually.

CREATE SEQUENCE BATCH_STEP_EXECUTION_SEQ MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE BATCH_JOB_EXECUTION_SEQ  MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE BATCH_JOB_SEQ            MAXVALUE 9223372036854775807 NO CYCLE;

CREATE TABLE BATCH_JOB_INSTANCE (
    JOB_INSTANCE_ID NUMBER(19,0)  NOT NULL PRIMARY KEY,
    VERSION         NUMBER(19,0),
    JOB_NAME        VARCHAR2(100) NOT NULL,
    JOB_KEY         VARCHAR2(32)  NOT NULL,
    CONSTRAINT JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)
);

CREATE TABLE BATCH_JOB_EXECUTION (
    JOB_EXECUTION_ID  NUMBER(19,0)   NOT NULL PRIMARY KEY,
    VERSION           NUMBER(19,0),
    JOB_INSTANCE_ID   NUMBER(19,0)   NOT NULL,
    CREATE_TIME       TIMESTAMP      NOT NULL,
    START_TIME        TIMESTAMP      DEFAULT NULL,
    END_TIME          TIMESTAMP      DEFAULT NULL,
    STATUS            VARCHAR2(10),
    EXIT_CODE         VARCHAR2(2500),
    EXIT_MESSAGE      VARCHAR2(2500),
    LAST_UPDATED      TIMESTAMP,
    CONSTRAINT JOB_INST_EXEC_FK FOREIGN KEY (JOB_INSTANCE_ID)
        REFERENCES BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
);

CREATE TABLE BATCH_JOB_EXECUTION_PARAMS (
    JOB_EXECUTION_ID NUMBER(19,0) NOT NULL,
    PARAMETER_NAME   VARCHAR2(100) NOT NULL,
    PARAMETER_TYPE   VARCHAR2(100) NOT NULL,
    PARAMETER_VALUE  VARCHAR2(2500),
    IDENTIFYING      CHAR(1) NOT NULL,
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
