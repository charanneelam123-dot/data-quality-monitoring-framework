# data-quality-monitoring-framework

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-FF6600)
![CI](https://img.shields.io/github/actions/workflow/status/your-org/data-quality-monitoring-framework/data_quality_check.yml?label=CI)

Reusable, **pipeline-agnostic data quality framework** built on PySpark. Plug into any Databricks notebook, AWS Glue job, or dbt post-hook. All thresholds are driven by YAML config — zero hardcoded values.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  YOUR PIPELINE  (Glue / Databricks / Spark Structured Streaming)        │
│                                                                         │
│  bronze_df = read_raw(...)   silver_df = transform(bronze_df)           │
│                                           │                             │
│            ┌──────────────────────────────┘                             │
│            ▼                                                            │
│  DQRunner.from_config("config/thresholds.yml", table_name="silver_x")  │
│            │                                                            │
│            ▼                                                            │
└────────────┬────────────────────────────────────────────────────────────┘
             │  runner.run(df, spark=spark)
             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  DQ ORCHESTRATOR  (orchestrator/dq_runner.py)                           │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Config resolution: tables["*"] deep-merged with tables[name]  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  Validator 1: NullRateValidator      per-column null % vs threshold     │
│  Validator 2: VolumeAnomalyDetector  count vs N-day rolling average     │
│  Validator 3: SchemaDriftDetector    new / missing / type-changed cols  │
│  Validator 4: FreshnessChecker       max(timestamp) vs SLA hours        │
│                                                                         │
│  ── Aggregate DQRunResult ──────────────────────────────────────────    │
│     overall_passed: bool                                                │
│     validator_results: list[dict]   (one per validator)                 │
│     run_id: UUID4                                                       │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  on failure (or notify_on_pass=true)
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  ALERT LAYER                                                            │
│                                                                         │
│  SlackNotifier  →  Block Kit rich message, severity colour, mentions    │
│  EmailNotifier  →  HTML email via SMTP, per-validator failure tables    │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  GREAT EXPECTATIONS (optional complement)                               │
│                                                                         │
│  base_suite.json          20 reusable expectations (override at runtime)│
│  pipeline_checkpoint.yml  RuntimeBatchRequest + Slack notification      │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  CI / CD (GitHub Actions)                                               │
│                                                                         │
│  lint ──▶ security ──▶ config-validation ──▶ unit-tests                │
│                                          └──▶ dq-sample-run ──▶ ci-gate│
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
data-quality-monitoring-framework/
├── validators/
│   ├── null_rate_validator.py      Per-column null % vs configurable thresholds
│   ├── volume_anomaly_detector.py  Row count vs 7-day rolling average
│   ├── schema_drift_detector.py   New / missing / type-changed columns
│   └── freshness_checker.py       max(timestamp) vs SLA window
├── alerts/
│   ├── slack_notifier.py          Block Kit rich Slack messages
│   └── email_notifier.py          HTML email via SMTP
├── orchestrator/
│   └── dq_runner.py               Single entry point — loads config, runs all validators
├── config/
│   ├── thresholds.yml             Master YAML config (all thresholds, all tables)
│   └── schemas/                   Auto-saved JSON baseline schemas (SchemaDriftDetector)
├── great_expectations/
│   ├── expectations/base_suite.json
│   └── checkpoints/pipeline_checkpoint.yml
├── tests/
│   └── test_validators.py         35+ pytest tests (local PySpark, no cloud deps)
├── .github/workflows/
│   └── data_quality_check.yml     5-job CI pipeline (blocks merge on any failure)
├── .env.example
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Install

```bash
git clone https://github.com/your-org/data-quality-monitoring-framework.git
cd data-quality-monitoring-framework
pip install -r requirements.txt
cp .env.example .env          # fill in SLACK_WEBHOOK_URL, SMTP_*, etc.
```

### 2. Configure thresholds

Edit `config/thresholds.yml`. Add your table under `tables:`:

```yaml
tables:
  "*":                          # global defaults
    validators:
      null_rate:
        global_threshold: 0.05
      freshness:
        timestamp_column: "_loaded_at"
        sla_hours: 26

  "orders.fact_orders":         # table-specific overrides
    validators:
      null_rate:
        global_threshold: 0.02
        column_thresholds:
          order_id: 0.0         # never null
```

### 3. Plug into your pipeline

```python
from orchestrator.dq_runner import DQRunner

# Works with any PySpark DataFrame — Databricks, Glue, local Spark
runner = DQRunner.from_config(
    "config/thresholds.yml",
    table_name="orders.fact_orders",
    pipeline_name="orders_etl",
)

result = runner.run(
    df,
    spark=spark,                          # only needed for volume history from Delta
    reference_counts={"2024-06-14": 50000, ...},   # or load from metrics table
)

if not result.overall_passed:
    raise RuntimeError(f"DQ failed — aborting. run_id={result.run_id}")

# Full structured result for audit logging:
print(result.to_dict())
```

### 4. Run tests locally

```bash
PYSPARK_PYTHON=$(which python) \
SPARK_LOCAL_IP=127.0.0.1 \
pytest tests/test_validators.py -v --cov=validators --cov=alerts --cov=orchestrator
```

---

## Validators

### NullRateValidator

Checks null percentage per column in a **single Spark pass** (no per-column actions).

| Config key | Default | Description |
|---|---|---|
| `global_threshold` | `0.05` | Max null rate for any column not in `column_thresholds` |
| `column_thresholds` | `{}` | Per-column override e.g. `{order_id: 0.0}` |
| `columns_to_skip` | `[]` | Columns excluded from validation |

### VolumeAnomalyDetector

Compares current row count against a rolling N-day average.

| Config key | Default | Description |
|---|---|---|
| `threshold_pct` | `0.30` | Max allowed variance e.g. `0.30` = ±30% |
| `lookback_days` | `7` | Days of history to average over |
| `min_baseline_days` | `3` | Skip check (warn only) if fewer days available |
| `metrics_table` | `null` | Optional Delta table with historical counts |

Supply history as a dict or let the detector load it from a Delta table:

```python
# Option A — supply directly (CI / first run)
result = detector.validate(df, reference_counts={"2024-06-14": 50000, ...})

# Option B — load from Delta metrics table automatically
result = detector.validate(df, spark=spark)
```

### SchemaDriftDetector

Compares current schema against a stored JSON baseline. Auto-saves baseline on first run.

| Config key | Default | Description |
|---|---|---|
| `reference_schema_path` | `null` | Path to JSON schema baseline file |
| `fail_on_new_columns` | `false` | New columns are warnings by default |
| `fail_on_missing_columns` | `true` | Missing columns always fail |
| `fail_on_type_changes` | `true` | Type changes always fail |
| `ignore_columns` | `[]` | Columns excluded (e.g. ETL audit cols) |

```python
# After a planned migration, update the baseline:
detector.save_reference_schema(df)
```

### FreshnessChecker

Checks that the newest record is within the SLA window.

| Config key | Default | Description |
|---|---|---|
| `timestamp_column` | `_loaded_at` | Column to find `MAX()` of |
| `sla_hours` | `26` | FAIL if newest record is older than this |
| `warn_hours` | `sla_hours × 0.75` | WARN (still passes) if approaching SLA |

---

## Alert Channels

### Slack (Block Kit)

```yaml
alerts:
  slack:
    webhook_url_env: SLACK_WEBHOOK_URL
    channel: "#data-quality"
    mention_on_failure: "@data-team"
    enabled: true
```

Produces rich Block Kit messages with:
- Colour-coded header (green/red)
- Per-validator failure details (up to 10 failures per validator)
- Summary count (`3/4 validators passed`)

### Email (SMTP + HTML)

```yaml
alerts:
  email:
    smtp_host_env: SMTP_HOST
    smtp_port: 587
    smtp_user_env: SMTP_USER
    smtp_password_env: SMTP_PASSWORD
    from_address: "dq-alerts@company.com"
    to_addresses: ["data-team@company.com"]
    send_on_pass: false
    enabled: true
```

Produces a responsive HTML email with a per-validator summary table and expandable failure details (capped at 25 rows per validator).

---

## CI / CD

The GitHub Actions pipeline has **5 jobs**. All must pass before a PR can be merged:

| Job | What it checks |
|---|---|
| `lint` | ruff, black, isort |
| `security` | bandit (HIGH severity blocks; MEDIUM reported) |
| `config-validation` | YAML structure, threshold ranges, .env.example completeness |
| `unit-tests` | 35+ pytest tests on local PySpark, coverage uploaded to Codecov |
| `dq-sample-run` | End-to-end DQ run on a 100-row synthetic DataFrame |
| `ci-gate` | Final AND gate — all above must succeed |

---

## Integration Examples

### Databricks Notebook (Silver layer)

```python
from orchestrator.dq_runner import DQRunner

silver_df = spark.table("silver.orders")

runner = DQRunner.from_config(
    "/dbfs/configs/dq/thresholds.yml",
    table_name="silver.orders",
    pipeline_name="medallion_pipeline",
)
result = runner.run(silver_df, spark=spark)

# Log to Delta audit table
spark.createDataFrame([result.to_dict()]).write.format("delta") \
    .mode("append").saveAsTable("dq.run_log")

if not result.overall_passed:
    dbutils.notebook.exit(json.dumps({"status": "DQ_FAILED", "run_id": result.run_id}))
```

### AWS Glue Job

```python
from orchestrator.dq_runner import DQRunner

runner = DQRunner.from_config(
    "s3://my-bucket/configs/dq/thresholds.yml",
    table_name="orders.fact_orders",
    pipeline_name="glue_etl_job",
    environment=os.environ.get("ENVIRONMENT", "prod"),
)
result = runner.run(valid_df, spark=spark)

if not result.overall_passed:
    logger.error("DQ failed: %s", result.to_dict())
    raise Exception(f"DQ checks failed — run_id={result.run_id}")
```

### dbt Post-Hook (via Python model)

```python
# models/python/validate_fact_orders.py
def model(dbt, session):
    df = dbt.ref("fact_orders")
    runner = DQRunner.from_config("config/thresholds.yml", table_name="fact_orders")
    result = runner.run(df)
    if not result.overall_passed:
        raise Exception(f"DQ failed: {result.failed_validators}")
    return df
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ENVIRONMENT` | No | `dev` / `staging` / `prod` (default: `dev`) |
| `SLACK_WEBHOOK_URL` | For Slack alerts | Slack incoming webhook URL |
| `SMTP_HOST` | For email alerts | SMTP server hostname |
| `SMTP_USER` | For email alerts | SMTP auth username |
| `SMTP_PASSWORD` | For email alerts | SMTP auth password |
| `DQ_CONFIG_PATH` | No | Override path to thresholds YAML |
| `DQ_METRICS_TABLE` | No | Delta table for volume history |

Copy `.env.example` → `.env` and populate. `python-dotenv` loads it automatically in local runs.
