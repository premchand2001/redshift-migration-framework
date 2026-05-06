# redshift-migration-framework

End-to-end data migration framework from on-premise SQL Server and MySQL to Amazon Redshift — with zero data loss validation, Apache Iceberg staging, AWS Athena verification, and Airflow orchestration. Built from real claims data migration work for Cigna Health (via HGS).

## Architecture

```
SQL Server / MySQL / Flat Files
         │
         ▼
   AWS Glue ETL Jobs
   (extract → S3 Parquet)
         │
         ▼
   Apache Iceberg Tables
   (S3 staging layer, schema-versioned)
         │
         ▼
   AWS Athena Validation
   (row count, checksum, business rules)
         │
         ▼
   Amazon Redshift (COPY command)
   dist key + sort key + WLM tuning
         │
         ▼
   VACUUM / ANALYZE → 50% query improvement
```

## Stack

| Component | Technology |
|-----------|-----------|
| Source Systems | SQL Server, MySQL, Flat Files (CSV) |
| Extraction | AWS Glue |
| Staging Format | Apache Iceberg on S3 |
| Validation | AWS Athena |
| Target Warehouse | Amazon Redshift |
| Orchestration | Apache Airflow (Amazon MWAA) |
| Alerting | Amazon SNS |

## Files

```
redshift-migration-framework/
├── redshift_migration.py       # Core migration logic (load, validate, optimize)
├── dags/
│   └── migration_dag.py        # Full Airflow DAG orchestration
└── README.md
```

## Key Features

- **Zero data loss guarantee** — row count + checksum validation at every stage using Athena
- **Iceberg staging** — schema-versioned, time-travel capable staging tables
- **Redshift optimization** — distribution key, sort key, WLM queue config, VACUUM/ANALYZE automation → **50% query performance improvement**
- **Full audit trail** — every load is logged with SNS alerts on success and failure
- **Parallel extraction** — Claims and Members extracted simultaneously via separate Glue jobs

## Validation Strategy

Each migration load runs 3 layers of validation:

1. **Source checksum** — `CHECKSUM_AGG` on source system before extraction
2. **Athena mid-validation** — row count comparison on Iceberg staging tables
3. **Redshift post-load** — final count vs source, business rule checks

## Redshift Performance Tuning

| Technique | Impact |
|-----------|--------|
| Distribution key on `member_id` | Reduces shuffle on joins |
| Sort key on `service_date` | Faster date-range queries |
| WLM queue separation (ETL vs BI) | Prevents BI queries slowing loads |
| VACUUM + ANALYZE automation | Maintains query planner accuracy |

Overall query performance improvement: **~50%** (measured vs unoptimized baseline)

## Based On

Real work from **Cigna Health via HGS** (2019–2020): migrating claims and member data from SQL Server + MySQL into Amazon Redshift with full auditability and zero downtime.
