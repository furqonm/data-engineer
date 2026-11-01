# Simple Processing with Cloud Composer

This guide covers the baseline Cloud Composer (Apache Airflow) DAG (`hadoop_tutorial.py`) from the lab. It provisions an ephemeral Dataproc cluster, runs a single Hadoop MapReduce `wordcount` job on `gs://pub/shakespeare/rose.txt`, and tears down the cluster once the job finishes.

---

## Airflow Variables Required

Before uploading the DAG, make sure these four Airflow Variables are configured in **Admin > Variables** in the Airflow web UI:

- `gcp_project`: Your active Google Cloud Project ID
- `gcs_bucket`: Your Cloud Storage bucket (e.g. `gs://<PROJECT_ID>`)
- `gce_region`: Compute Engine region (e.g. `us-central1`)
- `gce_zone`: Compute Engine zone (e.g. `us-central1-a`)

---

## DAG Script (`hadoop_tutorial.py`)

```python
# [START composer_hadoop_tutorial]
"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the Hadoop
wordcount example, and deletes the cluster.
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

# Output file for Cloud Dataproc job.
output_file = os.path.join(
    Variable.get('gcs_bucket'), 'wordcount',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep
# Path to Hadoop wordcount example available on every Dataproc cluster.
WORDCOUNT_JAR = (
    'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
)
# Arguments to pass to Cloud Dataproc job.
input_file = 'gs://pub/shakespeare/rose.txt'
wordcount_args = ['wordcount', input_file, output_file]

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

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        num_workers=2,
        region=Variable.get('gce_region'),
        zone=Variable.get('gce_zone'),
        image_version='2.1',
        master_machine_type='e2-standard-2',
        worker_machine_type='e2-standard-2')

    # Run the Hadoop wordcount example using the corrected operator.
    run_dataproc_hadoop = DataprocSubmitJobOperator(
        task_id='run_dataproc_hadoop',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),  # <-- CORRECTED: Changed 'location' back to 'region'
        job={
            'placement': {
                'cluster_name': 'composer-hadoop-tutorial-cluster-{{ ds_nodash }}'
            },
            'hadoop_job': {
                'main_jar_file_uri': WORDCOUNT_JAR,
                'args': wordcount_args,
            },
        },
    )

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=Variable.get('gcp_project'),
        region=Variable.get('gce_region'),
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        trigger_rule=TriggerRule.ALL_DONE)

    # [START composer_hadoop_steps]
    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster
    # [END composer_hadoop_steps]

# [END composer_hadoop_tutorial]
```

---

## How It Works

1. **`create_dataproc_cluster` (`DataprocCreateClusterOperator`):** Spins up a 2-worker Dataproc cluster named after the execution date (`composer-hadoop-tutorial-cluster-{{ ds_nodash }}`).
2. **`run_dataproc_hadoop` (`DataprocSubmitJobOperator`):** Runs the `wordcount` program from `hadoop-mapreduce-examples.jar` on `gs://pub/shakespeare/rose.txt` and writes the output to `<gcs_bucket>/wordcount/<timestamp>/`.
3. **`delete_dataproc_cluster` (`DataprocDeleteClusterOperator`):** Deletes the cluster after the job completes. Because it uses `trigger_rule=TriggerRule.ALL_DONE`, the cluster is deleted even if the Hadoop job fails, preventing orphaned VMs from accruing charges.
