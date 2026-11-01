# Simple ETL with Spark and BigQuery

Lab reference: [Use Serverless for Apache Spark to Load BigQuery](https://www.skills.google/focuses/102966?parent=catalog)

In the original lab, we take `campaigns.avro` and load it straight into BigQuery using the `GCSTOBIGQUERY` Dataproc template. That works, but nothing happens to the data in between—so participants often ask why we'd bother spinning up Spark when `bq load` can do the exact same thing in seconds.

For this demo, we use a custom PySpark script (`etl_campaign.py`) that cleans and reshapes the data before writing it to BigQuery, and run it as a batch job on Dataproc Serverless.

---

## The source data

`campaigns.avro` is small, just 10 rows of ad campaign data:

| id | created_at | period | campaign_name | amount | advertising_channel | bid_type |
|---|---|---|---|---|---|---|
| 81 | 2020-09-17 | 90 | NA - Video - Other | 41 | Video | CPC |
| 137 | 2021-01-19 | 30 | NA - Video - Promo | 325 | Video | CPC |
| 214 | 2021-06-28 | 30 | NA - Video - Promo | 78 | Video | CPC |
| 170 | 2021-03-15 | 30 | EU - Search - Brand | 465 | Search | CPC |
| 276 | 2022-01-01 | 30 | EU - Search - Brand | 83 | Search | CPC |
| 25 | 2020-02-18 | 30 | EU - Search - Brand | 30 | Search | CPC |
| 201 | 2021-06-08 | 30 | EU - Search - Brand | 172 | Search | CPC |
| 115 | 2020-11-29 | 60 | EU - Search - Other | 83 | Search | CPC |
| 237 | 2021-09-11 | 30 | EU - Search - Other | 86 | Search | CPC |
| 296 | 2022-02-17 | 30 | EU - Search - Other | 64 | Search | CPC |

A few things worth pointing out in the raw data:

- `created_at` is stored as a string, not a date.
- `campaign_name` packs three values into one field: region, channel, and category (e.g. `NA - Video - Promo`).
- `period` is the campaign length in days, so we can calculate when each campaign ends and how much it spent per day.

---

## Setup (run before the demo)

Swap in your own project ID and region if you are running a fresh lab session.

### In Cloud Shell

```bash
# Serverless Spark requires Private Google Access on the subnet
gcloud compute networks subnets update default --region=us-central1 --enable-private-ip-google-access

# One bucket for staging, one for BigQuery temp files
gsutil mb -p qwiklabs-gcp-02-cb89e6ac26cc gs://qwiklabs-gcp-02-cb89e6ac26cc

gsutil mb -p qwiklabs-gcp-02-cb89e6ac26cc gs://qwiklabs-gcp-02-cb89e6ac26cc-bqtemp

# Dataset for the output tables
bq mk -d loadavro
```

### On the remote VM

Go to **Compute Engine > VM instances** and SSH into `lab-vm`.

In this lab environment, the VM service account has restricted permissions on Cloud Storage buckets. Because of that, we don't upload local files from the VM to GCS. Instead, we download the files locally if we want to inspect them on the VM, and when we submit the Spark job, we point Dataproc directly to the public Cloud Storage buckets (`gs://furqon/etl_campaign.py` and `gs://cloud-training/.../campaigns.avro`).

If you want to download the files on the VM to inspect them during the demo:

```bash
# Download the Avro file and the PySpark ETL script locally to review
wget https://storage.googleapis.com/cloud-training/dataengineering/lab_assets/idegc/campaigns.avro
wget https://storage.googleapis.com/furqon/etl_campaign.py
```

---

## Walking through `etl_campaign.py`

The script is hosted at `gs://furqon/etl_campaign.py`. Here is the full code so we can walk through what it does step by step:

```python
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

def main(project_id, gcs_input_path, bq_dataset, temp_bucket):
    spark = SparkSession.builder \
        .appName("ETL-Avro-Campaigns-to-BigQuery") \
        .getOrCreate()

    # 1. Extract - read the Avro file from GCS
    logging.info(f"Reading Avro data from {gcs_input_path}")
    raw_df = spark.read.format("avro").load(gcs_input_path)
    raw_df.printSchema()
    raw_df.show(truncate=False)

    # 2. Transform - clean up and add new columns
    logging.info("Transforming campaign rows")
    transformed_df = (
        raw_df
        # "NA - Video - Promo" -> region "NA", category "Promo"
        .withColumn("market_region", trim(split(col("campaign_name"), "-").getItem(0)))
        .withColumn("campaign_category", trim(split(col("campaign_name"), "-").getItem(2)))
        # string -> date, then work out the end date
        .withColumn("start_date", to_date(col("created_at"), "yyyy-MM-dd"))
        .withColumn("end_date", expr("date_add(start_date, CAST(period AS INT))"))
        # spend per day and a simple budget bucket
        .withColumn("daily_spend", spark_round(col("amount") / col("period"), 2))
        .withColumn(
            "budget_tier",
            when(col("amount") >= 200, "High")
            .when(col("amount") >= 80, "Medium")
            .otherwise("Low")
        )
        .withColumn("etl_processed_at", current_timestamp())
    )
    transformed_df.show(truncate=False)

    # 3. Transform - summary per region / channel / category
    logging.info("Building summary table")
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
    summary_df.show(truncate=False)

    # 4. Load - write both tables to BigQuery
    detail_table = f"{project_id}.{bq_dataset}.campaigns"
    summary_table = f"{project_id}.{bq_dataset}.campaigns_summary"

    logging.info(f"Writing {detail_table}")
    transformed_df.write.format("bigquery") \
        .option("table", detail_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()

    logging.info(f"Writing {summary_table}")
    summary_df.write.format("bigquery") \
        .option("table", summary_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("overwrite") \
        .save()

    logging.info("Done")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: etl_campaign.py <PROJECT_ID> <GCS_INPUT_PATH> <BQ_DATASET> <TEMP_BUCKET>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
```

What happens in the four steps:

1. **Read.** Load `campaigns.avro` into a Spark DataFrame and print the schema and rows so we can see the raw input in the job logs.
2. **Clean up each row.**
   - Split `campaign_name` on `-` to pull out `market_region` (`NA`, `EU`) and `campaign_category` (`Brand`, `Promo`, `Other`).
   - Cast `created_at` to a `DATE` (`start_date`), then add `period` days to get `end_date`.
   - Compute `daily_spend` (`amount / period`) and assign a `budget_tier` (`High` for 200+, `Medium` for 80–199, `Low` for under 80).
   - Add an `etl_processed_at` timestamp.
3. **Summarize.** Group by region, channel, and category to calculate campaign count, total spend, and average daily spend.
4. **Write to BigQuery.** Save the cleaned rows to `loadavro.campaigns` and the aggregated numbers to `loadavro.campaigns_summary`.

---

## Running the job on Dataproc Serverless

From the VM (or Cloud Shell), run:

```bash
export GCP_PROJECT=qwiklabs-gcp-02-cb89e6ac26cc
export REGION=us-central1

gcloud dataproc batches submit pyspark gs://furqon/etl_campaign.py \
    --project=$GCP_PROJECT \
    --region=$REGION \
    --version=1.1 \
    --subnet=default \
    --jars=file:///usr/lib/spark/external/spark-avro.jar,gs://cloud-training/dataengineering/lab_assets/idegc/spark-bigquery_2.12-20221021-2134.jar \
    -- \
    $GCP_PROJECT \
    gs://cloud-training/dataengineering/lab_assets/idegc/campaigns.avro \
    loadavro \
    $GCP_PROJECT-bqtemp
```

A few notes on this command:

- Both the PySpark script (`gs://furqon/etl_campaign.py`) and the input data (`gs://cloud-training/.../campaigns.avro`) are read directly from public buckets, so we don't need `--deps-bucket` or any upload permissions from the VM.
- `--version=1.1` and the two JAR paths (`spark-avro.jar` and `spark-bigquery`) match the runtime setup expected by this lab environment.
- The four arguments after `--` are passed into `sys.argv` in `etl_campaign.py`: project ID, input Avro path, target BigQuery dataset, and the BigQuery temp bucket.

The batch takes a couple of minutes to finish provisioning. You can track it in the console under **Dataproc > Serverless > Batches** and check the output logs to see the `show()` tables printed by Spark.

---

## Checking the results in BigQuery

Once the batch status is `SUCCEEDED`, open BigQuery and query both tables.

Detail table (`loadavro.campaigns`):

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
FROM `loadavro.campaigns`
ORDER BY amount DESC;
```

Summary table (`loadavro.campaigns_summary`):

```sql
SELECT
  market_region,
  advertising_channel,
  campaign_category,
  total_campaigns,
  total_spend,
  avg_daily_spend,
  etl_processed_at
FROM `loadavro.campaigns_summary`
ORDER BY total_spend DESC;
```
