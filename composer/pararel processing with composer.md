# Parallel Processing with Cloud Composer

This guide extends the baseline `hadoop_tutorial.py` DAG to demonstrate **fan-out / fan-in parallel execution** in Cloud Composer (Apache Airflow) without needing any external resources beyond what the lab already allows.

Instead of running a single `wordcount` task sequentially, the DAG spins up the ephemeral Dataproc cluster once, runs **three different MapReduce jobs in parallel** using the built-in `hadoop-mapreduce-examples.jar` against `gs://pub/shakespeare/rose.txt`, and tears down the cluster only after all three jobs finish.

---

## Lab Setup (Run in Cloud Shell)

Run these commands in Cloud Shell to grant the necessary Composer Worker role, reset the required APIs, and create the Cloud Storage bucket named after your project ID:

```bash
export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# 1. Grant Composer worker role to the default Compute service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com \
  --role=roles/composer.worker

# 2. Reset required service APIs
gcloud services disable composer.googleapis.com
gcloud services disable artifactregistry.googleapis.com
gcloud services disable container.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable container.googleapis.com
gcloud services enable composer.googleapis.com

# 3. Create Cloud Storage bucket named after the project ID
gcloud storage buckets create gs://$PROJECT_ID --project=$PROJECT_ID
```

---

## Create and Upload `hadoop_tutorial.py` (Cloud Shell)

Run this block in Cloud Shell to create the parallel `hadoop_tutorial.py` locally and upload it directly to your bucket `gs://$PROJECT_ID`:

```bash
cat << 'EOF' > hadoop_tutorial.py
# [START composer_hadoop_tutorial]
"""Example Airflow DAG that creates an ephemeral Cloud Dataproc cluster,
runs three Hadoop MapReduce jobs in parallel (WordCount, WordMean, and Grep)
against gs://pub/shakespeare/rose.txt, and deletes the cluster when all jobs finish.
"""

from __future__ import annotations

import datetime
import os

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Base output timestamp for this DAG run.
run_timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
gcs_bucket = Variable.get('gcs_bucket')

# Separate output directories in GCS for each parallel Hadoop job.
wordcount_output = os.path.join(gcs_bucket, 'wordcount', run_timestamp) + os.sep
wordmean_output = os.path.join(gcs_bucket, 'wordmean', run_timestamp) + os.sep
grep_output = os.path.join(gcs_bucket, 'grep_rose', run_timestamp) + os.sep

# Path to Hadoop MapReduce examples JAR available on every Dataproc cluster.
EXAMPLES_JAR = (
    'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
)

# Public source file accessible in the lab environment.
input_file = 'gs://pub/shakespeare/rose.txt'

# Using a fixed start date is a best practice.
start_date = datetime.datetime(2025, 1, 1)

default_dag_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': Variable.get('gcp_project')
}

# [START composer_hadoop_schedule]
with DAG(
    'composer_hadoop_tutorial',
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
    catchup=False
) as dag:
    # [END composer_hadoop_schedule]

    cluster_name = 'composer-hadoop-tutorial-cluster-{{ ds_nodash }}'

    # 1. Create a Cloud Dataproc cluster.
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name=cluster_name,
        num_workers=2,
        region=Variable.get('gce_region'),
        zone=Variable.get('gce_zone'),
        image_version='2.1',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2')

    # 2A. Parallel Job 1: Count occurrences of every word (wordcount).
    run_wordcount = DataprocSubmitJobOperator(
        task_id='run_wordcount',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        job={
            'placement': {'cluster_name': cluster_name},
            'hadoop_job': {
                'main_jar_file_uri': EXAMPLES_JAR,
                'args': ['wordcount', input_file, wordcount_output],
            },
        },
    )

    # 2B. Parallel Job 2: Calculate average word length in the text (wordmean).
    run_wordmean = DataprocSubmitJobOperator(
        task_id='run_wordmean',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        job={
            'placement': {'cluster_name': cluster_name},
            'hadoop_job': {
                'main_jar_file_uri': EXAMPLES_JAR,
                'args': ['wordmean', input_file, wordmean_output],
            },
        },
    )

    # 2C. Parallel Job 3: Search and count pattern matches for "rose" (grep).
    run_grep_keyword = DataprocSubmitJobOperator(
        task_id='run_grep_keyword',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        job={
            'placement': {'cluster_name': cluster_name},
            'hadoop_job': {
                'main_jar_file_uri': EXAMPLES_JAR,
                'args': ['grep', input_file, grep_output, 'rose'],
            },
        },
    )

    # 3. Delete Cloud Dataproc cluster after all parallel jobs finish.
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_DONE)

    # [START composer_hadoop_steps]
    # Define fan-out / fan-in DAG dependencies.
    create_dataproc_cluster >> [run_wordcount, run_wordmean, run_grep_keyword] >> delete_dataproc_cluster
    # [END composer_hadoop_steps]

# [END composer_hadoop_tutorial]
EOF

# Upload hadoop_tutorial.py to Cloud Storage
gcloud storage cp hadoop_tutorial.py gs://$PROJECT_ID/
```

> [!TIP]
> If you are deploying this DAG into an existing Cloud Composer environment, you can copy the file directly to your Composer DAGs folder using:
> ```bash
> gcloud composer environments storage dags import \
>   --environment=<YOUR_COMPOSER_ENV_NAME> \
>   --location=<YOUR_REGION> \
>   --source=hadoop_tutorial.py
> ```

---

## What Runs in Parallel

`hadoop-mapreduce-examples.jar` on every Dataproc cluster bundles multiple MapReduce utilities. We run three of them side by side against `gs://pub/shakespeare/rose.txt`:

| Task ID | MapReduce Program | Arguments | Output Path in GCS (`gcs_bucket`) | What It Does |
|---|---|---|---|---|
| `run_wordcount` | `wordcount` | `['wordcount', input_file, wordcount_output]` | `<gcs_bucket>/wordcount/<timestamp>/` | Counts how many times each word appears in `rose.txt`. |
| `run_wordmean` | `wordmean` | `['wordmean', input_file, wordmean_output]` | `<gcs_bucket>/wordmean/<timestamp>/` | Calculates the average character length of words in `rose.txt`. |
| `run_grep_keyword` | `grep` | `['grep', input_file, grep_output, 'rose']` | `<gcs_bucket>/grep_rose/<timestamp>/` | Searches for and counts matches of the pattern `"rose"`. |

---

## DAG Script (`hadoop_tutorial.py`)

```python
# [START composer_hadoop_tutorial]
"""Example Airflow DAG that creates an ephemeral Cloud Dataproc cluster,
runs three Hadoop MapReduce jobs in parallel (WordCount, WordMean, and Grep)
against gs://pub/shakespeare/rose.txt, and deletes the cluster when all jobs finish.
"""

from __future__ import annotations

import datetime
import os

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Base output timestamp for this DAG run.
run_timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
gcs_bucket = Variable.get('gcs_bucket')

# Separate output directories in GCS for each parallel Hadoop job.
wordcount_output = os.path.join(gcs_bucket, 'wordcount', run_timestamp) + os.sep
wordmean_output = os.path.join(gcs_bucket, 'wordmean', run_timestamp) + os.sep
grep_output = os.path.join(gcs_bucket, 'grep_rose', run_timestamp) + os.sep

# Path to Hadoop MapReduce examples JAR available on every Dataproc cluster.
EXAMPLES_JAR = (
    'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
)

# Public source file accessible in the lab environment.
input_file = 'gs://pub/shakespeare/rose.txt'

# Using a fixed start date is a best practice.
start_date = datetime.datetime(2025, 1, 1)

default_dag_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': Variable.get('gcp_project')
}

# [START composer_hadoop_schedule]
with DAG(
    'composer_hadoop_tutorial',
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
    catchup=False
) as dag:
    # [END composer_hadoop_schedule]

    cluster_name = 'composer-hadoop-tutorial-cluster-{{ ds_nodash }}'

    # 1. Create a Cloud Dataproc cluster.
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name=cluster_name,
        num_workers=2,
        region=Variable.get('gce_region'),
        zone=Variable.get('gce_zone'),
        image_version='2.1',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2')

    # 2A. Parallel Job 1: Count occurrences of every word (wordcount).
    run_wordcount = DataprocSubmitJobOperator(
        task_id='run_wordcount',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        job={
            'placement': {'cluster_name': cluster_name},
            'hadoop_job': {
                'main_jar_file_uri': EXAMPLES_JAR,
                'args': ['wordcount', input_file, wordcount_output],
            },
        },
    )

    # 2B. Parallel Job 2: Calculate average word length in the text (wordmean).
    run_wordmean = DataprocSubmitJobOperator(
        task_id='run_wordmean',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        job={
            'placement': {'cluster_name': cluster_name},
            'hadoop_job': {
                'main_jar_file_uri': EXAMPLES_JAR,
                'args': ['wordmean', input_file, wordmean_output],
            },
        },
    )

    # 2C. Parallel Job 3: Search and count pattern matches for "rose" (grep).
    run_grep_keyword = DataprocSubmitJobOperator(
        task_id='run_grep_keyword',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        job={
            'placement': {'cluster_name': cluster_name},
            'hadoop_job': {
                'main_jar_file_uri': EXAMPLES_JAR,
                'args': ['grep', input_file, grep_output, 'rose'],
            },
        },
    )

    # 3. Delete Cloud Dataproc cluster after all parallel jobs finish.
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_DONE)

    # [START composer_hadoop_steps]
    # Define fan-out / fan-in DAG dependencies.
    create_dataproc_cluster >> [run_wordcount, run_wordmean, run_grep_keyword] >> delete_dataproc_cluster
    # [END composer_hadoop_steps]

# [END composer_hadoop_tutorial]
```

---

## Key Points to Show Participants

1. **Fan-out / Fan-in Dependency Syntax:**
   ```python
   create_dataproc_cluster >> [run_wordcount, run_wordmean, run_grep_keyword] >> delete_dataproc_cluster
   ```
   In the Airflow **Graph View**, participants will see `create_dataproc_cluster` branch out into three parallel tasks that run at the same time on the Dataproc cluster, then converge into `delete_dataproc_cluster`.
2. **Separate Output Directories:** Each parallel job writes to its own folder in GCS (`wordcount/`, `wordmean/`, and `grep_rose/`) because Hadoop MapReduce fails if two jobs try to create the same output directory simultaneously.
3. **`TriggerRule.ALL_DONE` on Teardown:** `delete_dataproc_cluster` waits until all three upstream jobs have finished (whether they succeeded or failed) before deleting the cluster.
