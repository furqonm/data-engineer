# 🚀 Simple ETL with Apache Spark and BigQuery

> **Lab Reference:** [Use Serverless for Apache Spark to Load BigQuery](https://www.skills.google/focuses/102966?parent=catalog)

In the standard lab scenario, you only perform a direct **EL (Extract & Load)** from an Avro file (`campaigns.avro`) into BigQuery using the pre-built `GCSTOBIGQUERY` Dataproc template. 

This guide extends the lab into a full **ETL (Extract, Transform, Load)** workflow using a standalone **PySpark** script (`etl_campaign.py`) on **Dataproc Serverless for Spark**, demonstrating why Apache Spark is used for data cleansing, feature engineering, and aggregation before loading into BigQuery.

---

## 📊 1. Source Dataset Overview (`campaigns.avro`)

The raw `campaigns.avro` file contains 10 marketing campaign records with the following schema and sample data:

| `id` (long) | `created_at` (string) | `period` (long) | `campaign_name` (string) | `amount` (long) | `advertising_channel` (string) | `bid_type` (string) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `81` | `"2020-09-17"` | `90` | `"NA - Video - Other"` | `41` | `"Video"` | `"CPC"` |
| `137` | `"2021-01-19"` | `30` | `"NA - Video - Promo"` | `325` | `"Video"` | `"CPC"` |
| `214` | `"2021-06-28"` | `30` | `"NA - Video - Promo"` | `78` | `"Video"` | `"CPC"` |
| `170` | `"2021-03-15"` | `30` | `"EU - Search - Brand"` | `465` | `"Search"` | `"CPC"` |
| `276` | `"2022-01-01"` | `30` | `"EU - Search - Brand"` | `83` | `"Search"` | `"CPC"` |
| `25` | `"2020-02-18"` | `30` | `"EU - Search - Brand"` | `30` | `"Search"` | `"CPC"` |
| `201` | `"2021-06-08"` | `30` | `"EU - Search - Brand"` | `172` | `"Search"` | `"CPC"` |
| `115` | `"2020-11-29"` | `60` | `"EU - Search - Other"` | `83` | `"Search"` | `"CPC"` |
| `237` | `"2021-09-11"` | `30` | `"EU - Search - Other"` | `86` | `"Search"` | `"CPC"` |
| `296` | `"2022-02-17"` | `30` | `"EU - Search - Other"` | `64` | `"Search"` | `"CPC"` |

---

## 🛠️ 2. Pre-Demo Setup (Execute Before Demo)

Run the following commands to prepare the network, Cloud Storage buckets, BigQuery dataset, and lab assets before walking through the PySpark ETL script.

> [!NOTE]
> Replace `qwiklabs-gcp-02-cb89e6ac26cc` and `us-central1` with your active Qwiklabs Project ID and Region if running in a new lab session.

### A. Cloud Shell Commands

Open **Cloud Shell** and run:

```bash
# 1. Enable Private IP Google Access on the default subnet (required by Serverless Spark)
gcloud compute networks subnets update default --region=us-central1 --enable-private-ip-google-access

# 2. Create the main staging bucket and BigQuery temporary bucket
gsutil mb -p qwiklabs-gcp-02-cb89e6ac26cc gs://qwiklabs-gcp-02-cb89e6ac26cc

gsutil mb -p qwiklabs-gcp-02-cb89e6ac26cc gs://qwiklabs-gcp-02-cb89e6ac26cc-bqtemp

# 3. Create the target BigQuery dataset
bq mk -d loadavro
```

### B. Remote VM (`lab-vm`) Commands

Open the SSH terminal to **`lab-vm`** (via **Compute Engine > VM instances > SSH**) and run:

```bash
# 1. Download the sample Avro dataset and upload it to Cloud Storage
wget https://storage.googleapis.com/cloud-training/dataengineering/lab_assets/idegc/campaigns.avro

gcloud storage cp campaigns.avro gs://qwiklabs-gcp-02-cb89e6ac26cc

# 2. Download and extract Dataproc Templates
wget https://storage.googleapis.com/cloud-training/dataengineering/lab_assets/idegc/dataproc-templates.zip

unzip dataproc-templates.zip

cd dataproc-templates/python
```

---

## 💻 3. Create and Explain `etl_campaign.py`

While inside `~/dataproc-templates/python` on the **Remote VM (`lab-vm`)**, create the `etl_campaign.py` file.

### What this ETL Script Does:
1. **Extract:** Reads the raw `campaigns.avro` file from Google Cloud Storage (`gs://<PROJECT_ID>/campaigns.avro`).
2. **Transform (Silver Layer - `loadavro.campaigns`):**
   * **String Splitting:** Splits `campaign_name` (e.g., `"NA - Video - Promo"`) using the `-` delimiter into two clean analytical columns: `market_region` (`NA`, `EU`) and `campaign_category` (`Other`, `Promo`, `Brand`).
   * **Date Casting & Arithmetic:** Casts `created_at` (`STRING`) into `start_date` (`DATE`) and calculates `end_date` by adding `period` (in days) to `start_date`.
   * **Feature Engineering:** Computes `daily_spend = round(amount / period, 2)` and categorizes campaigns into a `budget_tier` (`High` for $\ge 200$, `Medium` for $\ge 80$, and `Low` for $< 80$).
   * **Audit Column:** Adds `etl_processed_at` (`TIMESTAMP`) to record when the Spark job processed the row.
3. **Transform (Gold Layer - `loadavro.campaigns_summary`):**
   * Aggregates the enriched data by `market_region`, `advertising_channel`, and `campaign_category` to compute `total_campaigns`, `total_spend`, and `avg_daily_spend`.
4. **Load:** Writes both DataFrames directly to BigQuery using the built-in Spark BigQuery connector.

### Create `etl_campaign.py` on `lab-vm`:

```bash
cat << 'EOF' > etl_campaign.py
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, split, trim, expr, round as spark_round,
    when, current_timestamp, count, sum as spark_sum, avg
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def main(project_id: str, gcs_input_path: str, bq_dataset: str, temp_bucket: str):
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("ETL-Avro-Campaigns-to-BigQuery") \
        .getOrCreate()

    # ==========================================
    # 1. EXTRACT: Read Avro file from GCS
    # ==========================================
    logging.info(f"Reading raw Avro data from: {gcs_input_path}")
    raw_df = spark.read.format("avro").load(gcs_input_path)
    raw_df.printSchema()
    raw_df.show(truncate=False)

    # ==========================================
    # 2. TRANSFORM (Silver Layer): Cleansing & Feature Engineering
    # ==========================================
    logging.info("Transforming campaign records...")
    transformed_df = (
        raw_df
        # Split 'NA - Video - Promo' into market_region ('NA') and campaign_category ('Promo')
        .withColumn("market_region", trim(split(col("campaign_name"), "-").getItem(0)))
        .withColumn("campaign_category", trim(split(col("campaign_name"), "-").getItem(2)))
        # Convert created_at string to Date and calculate campaign end_date
        .withColumn("start_date", to_date(col("created_at"), "yyyy-MM-dd"))
        .withColumn("end_date", expr("date_add(start_date, CAST(period AS INT))"))
        # Calculate daily spend and classify budget tier
        .withColumn("daily_spend", spark_round(col("amount") / col("period"), 2))
        .withColumn(
            "budget_tier",
            when(col("amount") >= 200, "High")
            .when(col("amount") >= 80, "Medium")
            .otherwise("Low")
        )
        # Add ETL audit timestamp
        .withColumn("etl_processed_at", current_timestamp())
    )

    logging.info("Preview of Enriched Campaign Data (Silver):")
    transformed_df.show(truncate=False)

    # ==========================================
    # 3. TRANSFORM (Gold Layer): Aggregation Summary
    # ==========================================
    logging.info("Aggregating campaign metrics by region and channel...")
    summary_df = (
        transformed_df
        .groupBy("market_region", "advertising_channel", "campaign_category")
        .agg(
            count("id").alias("total_campaigns"),
            spark_sum("amount").alias("total_spend"),
            spark_round(avg("daily_spend"), 2).alias("avg_daily_spend")
        )
        .withColumn("etl_processed_at", current_timestamp())
    )

    logging.info("Preview of Aggregated Campaign Summary (Gold):")
    summary_df.show(truncate=False)

    # ==========================================
    # 4. LOAD: Write DataFrames to BigQuery
    # ==========================================
    silver_table = f"{project_id}.{bq_dataset}.campaigns"
    gold_table = f"{project_id}.{bq_dataset}.campaigns_summary"

    logging.info(f"Writing Silver table to BigQuery: {silver_table}")
    transformed_df.write.format("bigquery") \
        .option("table", silver_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()

    logging.info(f"Writing Gold table to BigQuery: {gold_table}")
    summary_df.write.format("bigquery") \
        .option("table", gold_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()

    logging.info("Spark ETL job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    # Expected arguments: <PROJECT_ID> <GCS_INPUT_PATH> <BQ_DATASET> <TEMP_BUCKET>
    if len(sys.argv) < 5:
        print("Usage: etl_campaign.py <PROJECT_ID> <GCS_INPUT_PATH> <BQ_DATASET> <TEMP_BUCKET>")
        sys.exit(1)

    main(
        project_id=sys.argv[1],
        gcs_input_path=sys.argv[2],
        bq_dataset=sys.argv[3],
        temp_bucket=sys.argv[4],
    )
EOF
```

---

## ▶️ 4. Execute the PySpark ETL Job on Dataproc Serverless

Still inside the **Remote VM (`lab-vm`)**, set the environment variables and submit `etl_campaign.py` to **Dataproc Serverless for Spark**:

```bash
# 1. Set environment variables
export GCP_PROJECT=qwiklabs-gcp-02-cb89e6ac26cc
export REGION=us-central1
export JARS="gs://cloud-training/dataengineering/lab_assets/idegc/spark-bigquery_2.12-20221021-2134.jar,file:///usr/lib/spark/connector/spark-avro.jar"

# 2. Submit the PySpark Serverless Batch Workload
gcloud dataproc batches submit pyspark etl_campaign.py \
    --project=$GCP_PROJECT \
    --region=$REGION \
    --deps-bucket=gs://$GCP_PROJECT \
    --subnet=default \
    --jars=$JARS \
    -- \
    $GCP_PROJECT \
    gs://$GCP_PROJECT/campaigns.avro \
    loadavro \
    $GCP_PROJECT-bqtemp
```

> [!TIP]
> **Monitoring Execution:**
> * In the **Google Cloud Console**, navigate to **Dataproc > Serverless > Batches** to show participants the live provisioning, driver logs (`Preview of Enriched Campaign Data`), and auto-scaling metrics.
> * Because `etl_campaign.py` writes the enriched records to `loadavro.campaigns`, the Qwiklabs **"Check my progress"** grader will also pass!

---

## 🔍 5. Verify Results in BigQuery

Once the batch job status turns **`SUCCEEDED`**, open **BigQuery Studio** in the Google Cloud Console (or run `bq query` in Cloud Shell) to inspect both tables generated by the Spark ETL pipeline:

### A. Query the Enriched Silver Table (`loadavro.campaigns`)

```sql
SELECT
  id,
  campaign_name,
  market_region,
  advertising_channel,
  campaign_category,
  start_date,
  end_date,
  period,
  amount,
  daily_spend,
  budget_tier,
  etl_processed_at
FROM
  `loadavro.campaigns`
ORDER BY
  amount DESC;
```

### B. Query the Aggregated Gold Table (`loadavro.campaigns_summary`)

```sql
SELECT
  market_region,
  advertising_channel,
  campaign_category,
  total_campaigns,
  total_spend,
  avg_daily_spend,
  etl_processed_at
FROM
  `loadavro.campaigns_summary`
ORDER BY
  total_spend DESC;
```
