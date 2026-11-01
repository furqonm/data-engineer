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
