# redshift-migration-framework

End-to-end data migration framework from on-premise SQL Server and MySQL to Amazon Redshift — zero data loss validation, Apache Iceberg staging, AWS Athena multi-layer verification, and Airflow orchestration. Built from real claims data migration work for **Cigna Health** (via HGS, 2019–2020).

---

## Why This Exists

Cigna's claims and member data lived across SQL Server, MySQL, and flat files with no unified analytical layer. BI teams ran queries directly on OLTP systems, ML teams had no clean training foundation, and there was no audit trail on data movement. This framework migrated everything into a properly tuned Amazon Redshift warehouse — with checksum validation at every stage so no record arrived corrupted or missing.

---

## Architecture

```
SQL Server / MySQL / Flat Files (CSV)
              │
              ▼
   AWS Glue ETL Jobs
   Dynamic frames + schema evolution handling
   Parallel extraction: Claims and Members simultaneously
              │
              ▼
   Apache Iceberg Tables on S3
   Schema-versioned staging layer
   Time-travel capable — rollback without re-extraction
              │
              ▼
   AWS Athena Validation
   Row count comparison + checksum verification
   Business rule checks before load proceeds
              │
              ▼
   Amazon Redshift  (COPY command)
   Distribution key + sort key + WLM tuning
              │
              ▼
   VACUUM / ANALYZE automation
   → 50% query performance improvement
```

---

## Stack

| Component | Technology |
|---|---|
| Source Systems | SQL Server, MySQL, Flat Files (CSV) |
| Extraction | AWS Glue (dynamic frames, parallel jobs) |
| Staging Format | Apache Iceberg on S3 |
| Validation | AWS Athena |
| Target Warehouse | Amazon Redshift |
| Orchestration | Apache Airflow (Amazon MWAA) |
| Alerting | Amazon SNS |

---

## Repository Structure

```
redshift-migration-framework/
├── redshift_migration.py        # Core migration logic — load, validate, optimize
├── dags/
│   └── migration_dag.py         # Airflow DAG — full orchestration
└── README.md
```

---

## Validation Strategy — 3 Layers

Every migration load runs three independent validation checks before the next stage proceeds. A failure at any layer halts the pipeline and fires an SNS alert.

| Stage | Check | Tool |
|---|---|---|
| **1 — Pre-extraction** | `CHECKSUM_AGG` on source system | SQL Server / MySQL query |
| **2 — Staging** | Row count comparison on Iceberg tables | AWS Athena |
| **3 — Post-load** | Final row count + business rule checks vs source | Redshift + Athena |

No silent failures. Every discrepancy is caught before downstream teams consume the data.

---

## Redshift Performance Tuning

After load, `redshift_migration.py` applies a structured tuning sequence:

| Technique | Why |
|---|---|
| Distribution key on `member_id` | Co-locates joins on the most common join key — reduces data shuffling across nodes |
| Sort key on `service_date` | Accelerates date-range queries used in BI and reporting |
| WLM queue separation (ETL vs BI) | Isolates heavy load jobs from interactive BI queries — prevents contention |
| Automated VACUUM + ANALYZE | Reclaims deleted space, keeps query planner statistics fresh after bulk loads |

**Result: ~50% query performance improvement** measured against pre-tuning baseline on the same query set.

---

## Iceberg Staging — Why Not Parquet Directly

Using Apache Iceberg over raw Parquet gives three things that matter here:

1. **Schema evolution** — source tables changed during the migration window. Iceberg absorbed column additions and type changes without requiring a full re-extraction.
2. **Time-travel** — if a Redshift load failed mid-way, we could roll back to a prior Iceberg snapshot and retry from staging rather than re-running the full Glue extraction.
3. **Row-level deletes** — lets us remove PII records from staging without rewriting entire partitions.

---

## Airflow DAG — `migration_dag.py`

```
extract_claims_parallel ──┐
extract_members_parallel ──┴── validate_iceberg_staging
                                    └── run_athena_validation
                                        └── load_redshift_claims
                                            └── load_redshift_members
                                                └── validate_post_load
                                                    └── run_vacuum_analyze
                                                        └── notify_success / notify_failure
```

- **Retries:** 3 retries with 10-minute delay on load steps
- **Alerting:** SNS on validation failure and on successful completion

---

## Setup

1. Configure AWS Glue connections to SQL Server / MySQL source systems
2. Create Iceberg tables on S3 with appropriate partition spec
3. Set up Athena workgroup for validation queries
4. Configure Redshift cluster — node type, WLM queue definitions
5. Deploy Airflow DAG to MWAA and set connections: `aws_default`, `redshift_prod`

---

## Based On

Real work for **Cigna Health** via HGS — Nov 2019 to Aug 2020. Migrated claims and member data from SQL Server + MySQL into Amazon Redshift with full auditability and zero data loss. The Redshift layer became the primary analytical foundation for Cigna's BI reporting and ML feature engineering teams.

---

## Author

**Premchand Kothapalli**
Senior AI / ML Engineer | AWS · Azure AI Foundry · LangGraph · PySpark
[LinkedIn](https://linkedin.com/in/pc-kothapalli) · premchandkdata@gmail.com · [GitHub](https://github.com/premchand2001)
