"""
Airflow DAG — Redshift Migration Orchestration
Claims & Member Data: SQL Server + MySQL → Amazon Redshift
Author: Premchand Kothapalli
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["data-alerts@yourcompany.com"],
}

with DAG(
    dag_id="redshift_migration_pipeline",
    default_args=default_args,
    description="Claims data migration from SQL Server/MySQL to Amazon Redshift via Iceberg + Athena",
    schedule_interval="@once",
    catchup=False,
    tags=["migration", "redshift", "iceberg", "healthcare", "cigna"],
) as dag:

    # ── Extract from SQL Server → S3 via Glue ─────────────────────────────────
    extract_claims = GlueJobOperator(
        task_id="extract_claims_sqlserver",
        job_name="extract-claims-sqlserver-to-s3",
        script_args={
            "--source_table": "dbo.claims",
            "--target_s3": "s3://your-migration-bucket/staging/claims/",
            "--file_format": "parquet",
        },
        aws_conn_id="aws_default",
        num_of_dpus=10,
    )

    extract_members = GlueJobOperator(
        task_id="extract_members_mysql",
        job_name="extract-members-mysql-to-s3",
        script_args={
            "--source_table": "members",
            "--target_s3": "s3://your-migration-bucket/staging/members/",
            "--file_format": "parquet",
        },
        aws_conn_id="aws_default",
        num_of_dpus=5,
    )

    # ── Register Iceberg tables in Glue Catalog ────────────────────────────────
    register_iceberg = AthenaOperator(
        task_id="register_iceberg_tables",
        query="""
            CREATE TABLE IF NOT EXISTS migration_catalog.claims_iceberg (
                claim_id       STRING,
                member_id      STRING,
                provider_id    STRING,
                service_date   DATE,
                diagnosis_code STRING,
                billed_amount  DOUBLE,
                paid_amount    DOUBLE
            )
            LOCATION 's3://your-migration-bucket/staging/claims/'
            TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');
        """,
        database="migration_catalog",
        output_location="s3://your-migration-bucket/athena-results/",
        aws_conn_id="aws_default",
    )

    # ── EMR: Process + validate via Iceberg + Athena ──────────────────────────
    process_steps = [
        {
            "Name": "Process and validate staging data",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit", "--deploy-mode", "cluster",
                    "s3://your-scripts-bucket/redshift_migration.py",
                    "--mode", "validate_staging",
                ],
            },
        }
    ]

    add_emr_step = EmrAddStepsOperator(
        task_id="add_processing_step",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=process_steps,
        aws_conn_id="aws_default",
    )

    emr_sensor = EmrStepSensor(
        task_id="wait_for_processing",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull('add_processing_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
    )

    # ── Load to Redshift via COPY ──────────────────────────────────────────────
    load_claims_redshift = SQLExecuteQueryOperator(
        task_id="load_claims_to_redshift",
        conn_id="redshift_default",
        sql="""
            COPY claims_fact
            FROM 's3://your-migration-bucket/staging/claims/'
            IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftS3AccessRole'
            FORMAT AS PARQUET
            COMPUPDATE ON
            STATUPDATE ON;
        """,
    )

    load_members_redshift = SQLExecuteQueryOperator(
        task_id="load_members_to_redshift",
        conn_id="redshift_default",
        sql="""
            COPY members_dim
            FROM 's3://your-migration-bucket/staging/members/'
            IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftS3AccessRole'
            FORMAT AS PARQUET
            COMPUPDATE ON
            STATUPDATE ON;
        """,
    )

    # ── Validate row counts + checksums ───────────────────────────────────────
    validate_counts = AthenaOperator(
        task_id="validate_row_counts",
        query="""
            SELECT
                'claims'  AS table_name,
                COUNT(*)  AS redshift_count
            FROM migration_catalog.claims_fact
            UNION ALL
            SELECT
                'members' AS table_name,
                COUNT(*)  AS redshift_count
            FROM migration_catalog.members_dim;
        """,
        database="migration_catalog",
        output_location="s3://your-migration-bucket/athena-results/validation/",
        aws_conn_id="aws_default",
    )

    # ── Optimize Redshift tables ──────────────────────────────────────────────
    optimize_tables = SQLExecuteQueryOperator(
        task_id="optimize_redshift_tables",
        conn_id="redshift_default",
        sql="""
            VACUUM SORT ONLY claims_fact;
            ANALYZE claims_fact;
            VACUUM SORT ONLY members_dim;
            ANALYZE members_dim;
        """,
    )

    # ── Success alert ─────────────────────────────────────────────────────────
    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn="arn:aws:sns:us-east-1:123456789:migration-alerts",
        message="✅ Redshift migration complete. Claims + Members tables loaded and validated.",
        subject="Migration SUCCESS",
        aws_conn_id="aws_default",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    [extract_claims, extract_members] >> register_iceberg >> add_emr_step >> emr_sensor
    emr_sensor >> [load_claims_redshift, load_members_redshift] >> validate_counts
    validate_counts >> optimize_tables >> notify_success
