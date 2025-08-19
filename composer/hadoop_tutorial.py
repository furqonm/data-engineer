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