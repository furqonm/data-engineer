#!/usr/bin/env python

"""
This script demonstrates a simple aggregation pipeline using Apache Beam.
It reads a CSV file from Google Cloud Storage, calculates the total amount
for each vendor, and writes the results to another GCS bucket.
"""

import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions

# Function to parse and filter the header from the CSV data.
# It yields (vendor_id, total_amount) tuples for valid rows.
def parse_and_filter_csv(line):
    # Skip the header row.
    if not line.startswith('vendor_id'):
        parts = line.split(',')
        try:
            # Assuming 'vendor_id' is at index 0 and 'total_amount' is at index 14.
            vendor_id = parts[0]
            total_amount = float(parts[14])
            # Yield the key-value pair.
            yield (vendor_id, total_amount)
        except (ValueError, IndexError) as e:
            # Log an error for malformed lines but don't stop the pipeline.
            logging.warning('Skipping malformed row: %s (Error: %s)', line, e)

def run():
    """Builds and runs the pipeline."""
    # Define and parse command-line arguments.
    # Using argparse makes the script more flexible and reusable.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        default='qwiklabs-gcp-03-396637544609',
        help='Your Google Cloud project ID.')
    parser.add_argument(
        '--bucket',
        default='qwiklabs-gcp-03-396637544609',
        help='Your Google Cloud Storage bucket name.')
    parser.add_argument(
        '--region',
        default='europe-west4',
        help='The region to run the Dataflow job in.')
    
    # Define the pipeline options, including all command-line arguments.
    known_args, pipeline_args = parser.parse_known_args()
    
    # DataflowRunner is a dynamic option, so we add it to the args.
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--staging_location=gs://{}/staging/'.format(known_args.bucket),
        '--temp_location=gs://{}/temp/'.format(known_args.bucket),
        '--project={}'.format(known_args.project),
        '--region={}'.format(known_args.region),
        '--job_name=aggregate-data-csv',
        '--save_main_session',
        '--worker_machine_type=e2-standard-2'
    ])
    
    # Create the pipeline with the provided options.
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as p:
        # Define input and output paths.
        input_file = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
        output_prefix = 'gs://{0}/csv_aggregation/output'.format(known_args.bucket)

        # The core pipeline logic.
        (p 
           | 'ReadCSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
           | 'ParseData' >> beam.FlatMap(parse_and_filter_csv)
           | 'GroupAndSum' >> beam.CombinePerKey(sum)
           | 'FormatOutput' >> beam.Map(lambda key_value: 'Vendor ID: {}, Total Amount: {}'.format(key_value[0], key_value[1]))
           | 'WriteToGCS' >> beam.io.WriteToText(output_prefix)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

