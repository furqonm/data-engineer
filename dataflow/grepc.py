#!/usr/bin/env python

"""
This script demonstrates a simple aggregation pipeline using Apache Beam.
It reads a CSV file from Google Cloud Storage, calculates the total amount
for each vendor, and writes the results to another GCS bucket.
"""

import apache_beam as beam

# This function parses each line of the CSV file and filters out the header.
# It uses a generator function (`yield`) to produce a `(key, value)` pair for each valid row.
def parse_and_filter_csv(line):
    # This line checks if the current line is the header row.
    if not line.startswith('vendor_id'):
        # Splits the comma-separated line into a list of strings.
        parts = line.split(',')
        try:
            # Extracts the vendor ID from the first column and total amount from the 15th column (index 14) and converts it to a float.
            vendor_id = parts[0]
            total_amount = float(parts[14])
            # `yield` produces a key-value pair, where the vendor ID is the key and the total amount is the value. This allows Apache Beam to process them.
            yield (vendor_id, total_amount)
        except (ValueError, IndexError):
            # This block handles potential errors, such as a row having missing or invalid data.
            # `ValueError` occurs if `float()` fails, and `IndexError` occurs if `parts` is too short.
            # `pass` ignores these invalid rows.
            pass

# Defines the Google Cloud Platform project, bucket, and region to be used.
PROJECT = 'qwiklabs-gcp-03-396637544609'
BUCKET = 'qwiklabs-gcp-03-396637544609'
REGION = 'europe-west4'

# This is the main function that sets up and runs the Apache Beam pipeline.
def run():
    # Defines the command-line arguments to be passed to the Beam pipeline.
    # These arguments are crucial for running the pipeline on Google Cloud Dataflow.
    argv = [
      # Sets a project, region location, and unique job name for the Dataflow job.
      '--project={0}'.format(PROJECT),
      '--job_name=aggregate-csv-lab',
      '--region={0}'.format(REGION),
      # This flag ensures that the main session is saved and made available to workers.
      '--save_main_session',
      # Specifies the location for staging and temporary files on GCS.
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      # Specifies that the pipeline will run on Google Cloud Dataflow and the machine type for the it's workers.
      '--worker_machine_type=e2-standard-2',
      '--runner=DataflowRunner'
    ]

    # Creates a Beam pipeline object with the defined command-line arguments.
    p = beam.Pipeline(argv=argv)
    
    # Defines the GCS path for the input CSV file and the output files.
    input_file = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
    output_prefix = 'gs://{0}/csv_aggregation/output'.format(BUCKET)

    # This is the core of the Beam pipeline, where the data processing steps are chained together.
    (p 
        # Reads data from the specified GCS file.
        | 'GetCSV' >> beam.io.ReadFromText(input_file)
        # Applies the `parse_and_filter_csv` function to each line.
        # `FlatMap` is used because the function might return zero or more elements for each input line.
        | 'ParseAndFilter' >> beam.FlatMap(parse_and_filter_csv)
        # Groups the elements by key (vendor ID) and then sums the values (total amount) for each group.
        # This is a key-value aggregation operation.
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        # Formats the aggregated key-value pairs into a human-readable string.
        | 'FormatOutput' >> beam.Map(lambda key_value: 'Vendor ID: {}, Total Amount: {}'.format(key_value[0], key_value[1]))
        # Writes the formatted strings to the specified GCS output path.
        | 'write' >> beam.io.WriteToText(output_prefix)
    )

    # Runs the entire pipeline.
    p.run()

# This is a standard Python construct that ensures the `run()` function is called
# only when the script is executed directly, not when it's imported as a module.
if __name__ == '__main__':
    run()