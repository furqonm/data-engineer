#!/usr/bin/env python

"""
This script demonstrates a simple aggregation pipeline using Apache Beam.
It reads a CSV file from Google Cloud Storage, calculates the total amount
for each vendor, and writes the results to another GCS bucket.
"""

import apache_beam as beam

def parse_and_filter_csv(line):
    if not line.startswith('vendor_id'):
        parts = line.split(',')
        try:
            vendor_id = parts[0]
            total_amount = float(parts[14])
            yield (vendor_id, total_amount)
        except (ValueError, IndexError):
            pass

PROJECT = 'qwiklabs-gcp-03-396637544609'
BUCKET = 'qwiklabs-gcp-03-396637544609'
REGION = 'europe-west4'

def run():
    argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=aggregate-csv',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--region={0}'.format(REGION),
      '--runner=DataflowRunner',
      '--worker_machine_type=e2-standard-2',
      '--sdk_container_image=gcr.io/cloud-dataflow/python/flex-image-py310-apache-beam-2.66.0:latest',
      '--experiments=use_runner_v2',
      '--sdk_location=container'
    ]

    # Important: Ensure your local environment has the same SDK version
    # Before running the script, execute this command in your terminal:
    # pip install apache-beam[gcp]==2.66.0

    p = beam.Pipeline(argv=argv)
    
    input_file = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
    output_prefix = 'gs://{0}/csv_aggregation/output'.format(BUCKET)

    (p 
        | 'GetCSV' >> beam.io.ReadFromText(input_file)
        | 'ParseAndFilter' >> beam.FlatMap(parse_and_filter_csv)
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'FormatOutput' >> beam.Map(lambda key_value: 'Vendor ID: {}, Total Amount: {}'.format(key_value[0], key_value[1]))
        | 'write' >> beam.io.WriteToText(output_prefix)
    )

    p.run()

if __name__ == '__main__':
    run()