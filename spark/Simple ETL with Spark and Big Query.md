# Simple ETL with Spark and BigQuery

Lab reference: [Use Serverless for Apache Spark to Load BigQuery](https://www.skills.google/focuses/102966?parent=catalog)

In the original lab, we take `campaigns.avro` and load it into BigQuery using the `GCSTOBIGQUERY` Dataproc template. That works, but it's really just extract and load. Nothing happens to the data in between, so it's fair to ask why we'd use Spark at all when `bq load` can do the same thing.

So for this demo I wrote a small PySpark script, `etl_campaign.py`, that cleans and reshapes the data before it lands in BigQuery. We'll run it as a batch job on Dataproc Serverless.

---

## The source data

`campaigns.avro` is tiny, just 10 rows of ad campaign data:

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

Some things to point out:

- `created_at` is stored as a string, not a date.
- `campaign_name` holds three values in one field: region, channel, and category.
- `period` is the campaign length in days, so we can work out when each campaign ends and how much it spent per day.

The script fixes all of these.

---

## Setup (run this before the demo)

My project ID and region are below. If you're starting a new lab session, swap in your own.

### In Cloud Shell

```bash
# Serverless Spark needs Private Google Access on the subnet
gcloud compute networks subnets update default --region=us-central1 --enable-private-ip-google-access

# One bucket for the data and staging files, one for BigQuery temp files
gsutil mb -p qwiklabs-gcp-02-cb89e6ac26cc gs://qwiklabs-gcp-02-cb89e6ac26cc

gsutil mb -p qwiklabs-gcp-02-cb89e6ac26cc gs://qwiklabs-gcp-02-cb89e6ac26cc-bqtemp

# Dataset for the output tables
bq mk -d loadavro
```

### On the remote VM

Go to Compute Engine > VM instances and SSH into `lab-vm`. Then run:

```bash
# Get the Avro file and copy it to the bucket
wget https://storage.googleapis.com/cloud-training/dataengineering/lab_assets/idegc/campaigns.avro

gcloud storage cp campaigns.avro gs://qwiklabs-gcp-02-cb89e6ac26cc

# Get the Dataproc templates (we'll work inside this folder)
wget https://storage.googleapis.com/cloud-training/dataengineering/lab_assets/idegc/dataproc-templates.zip

unzip dataproc-templates.zip

cd dataproc-templates/python
```

---

## Walking through etl_campaign.py

The script has four steps.

**1. Read.** Load the Avro file from GCS into a DataFrame. We print the schema and the rows first so we can see what Spark picked up.

**2. Clean up each row.** This part does the most work:

- Split `campaign_name` on `-` to get `market_region` (NA / EU) and `campaign_category` (Brand / Promo / Other). We don't need a separate channel column because `advertising_channel` already exists.
- Convert `created_at` to a real `DATE` called `start_date`, then add `period` days to get `end_date`.
- Calculate `daily_spend` as `amount / period`.
- Put each campaign in a `budget_tier`: 200 and above is High, 80 to 199 is Medium, below 80 is Low.
- Add `etl_processed_at` so we know when the row was processed. This is a good habit in any pipeline.

**3. Summarize.** Group by region, channel, and category, then calculate the campaign count, total spend, and average daily spend. This is the kind of table a dashboard would read from.

**4. Write to BigQuery.** The detailed rows go to `loadavro.campaigns` and the summary goes to `loadavro.campaigns_summary`. One job, two tables.

I kept the table name `campaigns` on purpose, because that's the table the original lab creates. That way the lab's progress check should still find it.

To create the file on the VM, paste this whole block:

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
EOF
```

---

## Running it on Dataproc Serverless

Still on the VM. First, copy the script to the bucket:

```bash
export GCP_PROJECT=qwiklabs-gcp-02-cb89e6ac26cc
export REGION=us-central1

gcloud storage cp etl_campaign.py gs://$GCP_PROJECT/scripts/etl_campaign.py
```

Why not just submit the local file? If you pass a local `.py` file, gcloud has to upload it with `--deps-bucket`, and that first reads the bucket's metadata (`storage.buckets.get`). The VM runs as the Compute Engine default service account, and in this lab it can read and write objects but can't read bucket metadata. You'll get a permission error if you try. Uploading the script yourself with `gcloud storage cp` only needs object access, which is the same thing we did with `campaigns.avro`.

Then submit the batch, pointing at the script in GCS:

```bash
gcloud dataproc batches submit pyspark gs://$GCP_PROJECT/scripts/etl_campaign.py \
    --project=$GCP_PROJECT \
    --region=$REGION \
    --version=1.1 \
    --subnet=default \
    --jars=file:///usr/lib/spark/external/spark-avro.jar,gs://cloud-training/dataengineering/lab_assets/idegc/spark-bigquery_2.12-20221021-2134.jar \
    -- \
    $GCP_PROJECT \
    gs://$GCP_PROJECT/campaigns.avro \
    loadavro \
    $GCP_PROJECT-bqtemp
```

Notes on the flags:

- No `--deps-bucket`. The script and the JARs are already in GCS (or on the runtime image), so gcloud has nothing to upload.
- `--version=1.1` and the `spark-avro.jar` path are the same ones the lab's own `bin/start.sh` uses. The BigQuery connector JAR is also the one the lab provides. I kept them identical so we know they work in this environment.
- The four values after `--` are passed to the script as arguments: project, input file, dataset, and temp bucket.

The job takes a couple of minutes to start because Dataproc has to provision resources first. While you wait, open Dataproc > Serverless > Batches in the console and show participants the job. The output tab will show the `show()` results from each step, which helps connect the code to the data.

---

## Checking the results

When the batch shows `SUCCEEDED`, go to BigQuery and run:

Detail table:

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

Summary table:

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
