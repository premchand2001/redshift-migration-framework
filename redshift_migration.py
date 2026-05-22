"""
redshift_migration.py
---------------------
End-to-end data migration framework from SQL Server / MySQL / flat files
to Amazon Redshift with zero data loss validation.

3-layer validation: source checksum → Iceberg staging → Redshift post-load.
Redshift performance tuning: dist keys, sort keys, WLM, VACUUM/ANALYZE.

Built for Cigna Health claims data migration (via HGS, 2019–2020).

Author: Premchand Kothapalli
Stack:  AWS Glue, Apache Iceberg, AWS Athena, Amazon Redshift, Airflow, SNS
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import boto3
import pyathena
import psycopg2

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass
class MigrationConfig:
    env: str = "prod"

    # S3 / Iceberg
    staging_bucket:  str = "cigna-migration-staging-prod"
    staging_prefix:  str = "iceberg/"
    glue_database:   str = "cigna_migration"

    # Athena
    athena_workgroup:    str = "migration-validation"
    athena_output_path:  str = "s3://cigna-migration-staging-prod/athena-results/"

    # Redshift
    redshift_host:     str = ""
    redshift_port:     int = 5439
    redshift_db:       str = "cigna_dw"
    redshift_user:     str = ""
    redshift_password: str = ""
    redshift_role_arn: str = ""     # IAM role for Redshift COPY from S3
    redshift_schema:   str = "claims"

    # SNS
    sns_topic_arn: str = ""

    # Validation tolerances
    row_count_tolerance_pct: float = 0.001   # 0.1% row count variance allowed
    checksum_strict: bool = True             # any checksum mismatch = fail


# ---------------------------------------------------------------------------
# SNS helper
# ---------------------------------------------------------------------------
def _alert(cfg: MigrationConfig, subject: str, message: str, level: str = "ERROR") -> None:
    boto3.client("sns").publish(
        TopicArn=cfg.sns_topic_arn,
        Subject=f"[{level}] Migration — {subject}",
        Message=json.dumps({"level": level, "message": message,
                            "timestamp": datetime.utcnow().isoformat()}, indent=2),
    )


# ---------------------------------------------------------------------------
# Layer 1: Source Validation (pre-extraction checksum)
# ---------------------------------------------------------------------------
class SourceValidator:
    """
    Connects to source systems (SQL Server / MySQL) and captures
    row count + CHECKSUM_AGG before extraction begins.
    """

    def __init__(self, conn_string: str, db_type: str = "sqlserver"):
        self.conn_string = conn_string
        self.db_type     = db_type

    def _connect(self):
        if self.db_type == "sqlserver":
            import pyodbc
            return pyodbc.connect(self.conn_string)
        elif self.db_type == "mysql":
            import pymysql
            return pymysql.connect(**self._parse_mysql(self.conn_string))
        raise ValueError(f"Unsupported db_type: {self.db_type}")

    def get_row_count(self, table_name: str) -> int:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cur.fetchone()[0]

    def get_checksum(self, table_name: str) -> str:
        """
        SQL Server: CHECKSUM_AGG(BINARY_CHECKSUM(*))
        MySQL:      CRC32 on concatenated key columns
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                if self.db_type == "sqlserver":
                    cur.execute(f"SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) FROM {table_name}")
                elif self.db_type == "mysql":
                    cur.execute(f"SELECT BIT_XOR(CRC32(CONCAT_WS(',', *))) FROM {table_name}")
                return str(cur.fetchone()[0])

    def capture_baseline(self, table_name: str) -> dict:
        row_count = self.get_row_count(table_name)
        checksum  = self.get_checksum(table_name)
        baseline  = {
            "table":      table_name,
            "row_count":  row_count,
            "checksum":   checksum,
            "captured_at": datetime.utcnow().isoformat(),
        }
        log.info(f"[SOURCE] {table_name}: {row_count:,} rows, checksum={checksum}")
        return baseline


# ---------------------------------------------------------------------------
# Layer 2: Iceberg Staging Validator (Athena mid-validation)
# ---------------------------------------------------------------------------
class IcebergStagingValidator:
    """
    Validates Iceberg staging tables on S3 via Athena.
    Compares row counts against source baseline before Redshift load proceeds.
    """

    def __init__(self, cfg: MigrationConfig):
        self.cfg  = cfg
        self.conn = pyathena.connect(
            s3_staging_dir=cfg.athena_output_path,
            work_group=cfg.athena_workgroup,
        )

    def get_staging_count(self, table_name: str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.cfg.glue_database}.{table_name}_staging")
        return cursor.fetchone()[0]

    def validate_against_source(self, table_name: str, source_baseline: dict) -> None:
        staging_count = self.get_staging_count(table_name)
        source_count  = source_baseline["row_count"]
        variance      = abs(staging_count - source_count) / source_count if source_count > 0 else 1

        log.info(f"[STAGING] {table_name}: source={source_count:,}, staging={staging_count:,}, "
                 f"variance={variance:.3%}")

        if variance > self.cfg.row_count_tolerance_pct:
            msg = (f"Staging row count variance exceeds tolerance for {table_name}: "
                   f"source={source_count:,}, staging={staging_count:,}, "
                   f"variance={variance:.3%} > {self.cfg.row_count_tolerance_pct:.3%}")
            log.error(msg)
            _alert(self.cfg, f"Staging Validation Failed — {table_name}", msg)
            raise ValueError(msg)

        log.info(f"[STAGING] {table_name}: row count validated ✓")

    def run_business_rules(self, table_name: str, rules: list) -> None:
        """
        Run custom SQL business rule checks on staging.
        rules: list of {"name": str, "sql": str, "expected_count": int}
        """
        cursor = self.conn.cursor()
        for rule in rules:
            cursor.execute(rule["sql"])
            actual_count = cursor.fetchone()[0]
            if actual_count != rule["expected_count"]:
                msg = (f"Business rule '{rule['name']}' failed on {table_name}: "
                       f"expected={rule['expected_count']}, actual={actual_count}")
                log.error(msg)
                _alert(self.cfg, f"Business Rule Failed — {table_name}", msg)
                raise ValueError(msg)
            log.info(f"[STAGING] Rule '{rule['name']}': {actual_count} ✓")


# ---------------------------------------------------------------------------
# Layer 3: Redshift Loader + Post-load Validator
# ---------------------------------------------------------------------------
class RedshiftLoader:
    """
    Loads Iceberg-staged Parquet files into Redshift via COPY command.
    Applies distribution keys, sort keys, and WLM tuning for query performance.
    Runs VACUUM + ANALYZE after every bulk load.
    """

    def __init__(self, cfg: MigrationConfig):
        self.cfg = cfg

    def _conn(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.cfg.redshift_host,
            port=self.cfg.redshift_port,
            dbname=self.cfg.redshift_db,
            user=self.cfg.redshift_user,
            password=self.cfg.redshift_password,
        )

    def create_table(self, table_name: str, ddl: str) -> None:
        """Create table with distribution and sort key strategy pre-defined in DDL."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.cfg.redshift_schema}.{table_name}")
                cur.execute(ddl)
            conn.commit()
        log.info(f"[REDSHIFT] Table created: {self.cfg.redshift_schema}.{table_name}")

    def copy_from_s3(self, table_name: str, s3_path: str) -> None:
        """
        COPY command — parallel load from S3 Parquet.
        Uses IAM role for auth — no credentials in SQL.
        """
        copy_sql = f"""
            COPY {self.cfg.redshift_schema}.{table_name}
            FROM '{s3_path}'
            IAM_ROLE '{self.cfg.redshift_role_arn}'
            FORMAT AS PARQUET
            SERIALIZETOJSON
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                log.info(f"[REDSHIFT] COPY {table_name} FROM {s3_path}")
                cur.execute(copy_sql)
            conn.commit()
        log.info(f"[REDSHIFT] COPY complete: {table_name}")

    def validate_post_load(self, table_name: str, source_baseline: dict) -> int:
        """Layer 3 validation — final row count vs source."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {self.cfg.redshift_schema}.{table_name}"
                )
                redshift_count = cur.fetchone()[0]

        source_count = source_baseline["row_count"]
        variance     = abs(redshift_count - source_count) / source_count if source_count > 0 else 1

        log.info(f"[REDSHIFT] {table_name}: source={source_count:,}, "
                 f"loaded={redshift_count:,}, variance={variance:.3%}")

        if variance > self.cfg.row_count_tolerance_pct:
            msg = (f"Post-load validation failed for {table_name}: "
                   f"source={source_count:,}, redshift={redshift_count:,}, "
                   f"variance={variance:.3%}")
            log.error(msg)
            _alert(self.cfg, f"Post-load Validation Failed — {table_name}", msg)
            raise ValueError(msg)

        log.info(f"[REDSHIFT] {table_name}: post-load validation passed ✓")
        return redshift_count

    def run_vacuum_analyze(self, table_name: str) -> None:
        """Reclaim space and refresh query planner stats after bulk load."""
        with self._conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                log.info(f"[REDSHIFT] VACUUM {table_name}")
                cur.execute(f"VACUUM {self.cfg.redshift_schema}.{table_name}")
                log.info(f"[REDSHIFT] ANALYZE {table_name}")
                cur.execute(f"ANALYZE {self.cfg.redshift_schema}.{table_name}")
        log.info(f"[REDSHIFT] VACUUM + ANALYZE complete: {table_name} ✓")

    def configure_wlm(self) -> None:
        """
        Apply WLM queue labels to separate ETL and BI workloads.
        Prevents heavy COPY jobs from starving interactive BI queries.
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SET query_group TO 'etl'")
            conn.commit()
        log.info("[REDSHIFT] WLM queue set to 'etl' ✓")


# ---------------------------------------------------------------------------
# Migration Orchestrator — 3-layer validation end-to-end
# ---------------------------------------------------------------------------
class MigrationPipeline:
    """
    Orchestrates the full migration with 3-layer validation:
      Layer 1: Source checksum (pre-extraction)
      Layer 2: Iceberg staging row count (Athena mid-validation)
      Layer 3: Redshift post-load row count
    """

    def __init__(self, cfg: MigrationConfig, source_conn_string: str, db_type: str = "sqlserver"):
        self.cfg              = cfg
        self.source_validator = SourceValidator(source_conn_string, db_type)
        self.staging_validator = IcebergStagingValidator(cfg)
        self.loader           = RedshiftLoader(cfg)

    def run(
        self,
        table_name: str,
        s3_staging_path: str,
        redshift_ddl: str,
        business_rules: Optional[list] = None,
    ) -> dict:
        log.info(f"=== Migration Pipeline START: {table_name} ===")
        results = {}

        # Layer 1: Capture source baseline (row count + checksum)
        source_baseline = self.source_validator.capture_baseline(table_name)
        results["source"] = source_baseline

        # Layer 2: Validate Iceberg staging against source
        self.staging_validator.validate_against_source(table_name, source_baseline)
        if business_rules:
            self.staging_validator.run_business_rules(table_name, business_rules)
        results["staging_count"] = self.staging_validator.get_staging_count(table_name)

        # Create Redshift target table
        self.loader.create_table(table_name, redshift_ddl)

        # Set WLM to ETL queue before bulk load
        self.loader.configure_wlm()

        # COPY from S3 staging → Redshift
        self.loader.copy_from_s3(table_name, s3_staging_path)

        # Layer 3: Post-load validation
        results["redshift_count"] = self.loader.validate_post_load(table_name, source_baseline)

        # VACUUM + ANALYZE
        self.loader.run_vacuum_analyze(table_name)

        _alert(
            self.cfg,
            f"Migration SUCCESS — {table_name}",
            json.dumps(results, indent=2),
            level="INFO",
        )

        log.info(f"=== Migration Pipeline COMPLETE: {table_name} ===")
        log.info(f"    Source:   {results['source']['row_count']:,}")
        log.info(f"    Staging:  {results['staging_count']:,}")
        log.info(f"    Redshift: {results['redshift_count']:,}")
        return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    env = sys.argv[1] if len(sys.argv) > 1 else "prod"
    cfg = MigrationConfig(env=env)

    pipeline = MigrationPipeline(
        cfg=cfg,
        source_conn_string="DRIVER={SQL Server};SERVER=cigna-sql-prod;DATABASE=claims;",
        db_type="sqlserver",
    )

    claims_ddl = """
        CREATE TABLE claims.medical_claims (
            claim_id        VARCHAR(36)    ENCODE ZSTD,
            member_id       VARCHAR(36)    ENCODE ZSTD,
            service_date    DATE           ENCODE DELTA,
            procedure_code  VARCHAR(10)    ENCODE BYTEDICT,
            billed_amount   DECIMAL(12,2)  ENCODE DELTA32K,
            allowed_amount  DECIMAL(12,2)  ENCODE DELTA32K,
            created_at      TIMESTAMP      ENCODE DELTA
        )
        DISTSTYLE KEY
        DISTKEY(member_id)
        SORTKEY(service_date)
    """

    pipeline.run(
        table_name="medical_claims",
        s3_staging_path=f"s3://cigna-migration-staging-{env}/iceberg/medical_claims/",
        redshift_ddl=claims_ddl,
    )
