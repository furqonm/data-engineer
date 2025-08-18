#!/usr/bin/env python

"""
Apache Beam pipeline to be run on Google Cloud Dataflow.
It reads a CSV file from GCS, calculates the average trip distance
for each passenger count, and writes the results to a new file in GCS.
"""

import apache_beam as beam
import sys

# Define your Google Cloud configuration
# You MUST update these values with your specific project and bucket details.
# The REGION is provided by your qwiklabs environment.
PROJECT = 'cloud-training-demos' 
BUCKET = 'cloud-training-demos'
REGION = 'qwiklabs-provided-region'

# GCS path for the input file and output directory
INPUT_FILE = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
OUTPUT_PREFIX = 'gs://{0}/avg_distance_by_passengers/output'.format(BUCKET)

def parse_csv_and_prepare_for_agg(line):
    """
    Parses a CSV line and returns a (key, value) tuple for aggregation.
    Key: passenger_count (int)
    Value: trip_distance (float)
    """
    try:
        fields = line.split(',')
        # Return a (key, value) tuple where key is passenger count
        # and value is trip distance.
        # This will be used by beam.Mean.PerKey().
        return (int(fields[0]), float(fields[1]))
    except (ValueError, IndexError):
        # Ignore malformed lines and the header
        return None

def run():
    """
    Configures and runs the Apache Beam pipeline on Dataflow.
    """
    # Create the pipeline options
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=calculate-avg-distance-job',  # A unique job name is required
        '--save_main_session', # Required for some custom classes/functions
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        '--region={0}'.format(REGION),
        '--runner=DataflowRunner'
        '--setup_file=./setup.py'
    ]

    # Create the pipeline object with the defined options
    with beam.Pipeline(argv=argv) as p:
        # Step 1: Read, Parse, and Prepare the data.
        keyed_data = (
            p 
            | 'ReadFromGCS' >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            | 'ParseAndKey' >> beam.Map(parse_csv_and_prepare_for_agg)
            | 'FilterNone' >> beam.Filter(lambda item: item is not None)
        )
        
        # Step 2: Calculate the average for each key using a powerful pre-built transform.
        average_distance_per_passenger = (
            keyed_data
            | 'CalculateAverage' >> beam.Mean.PerKey()
        )
        
        # Step 3: Format the results for writing to GCS.
        # The output of beam.Mean.PerKey() is a PCollection of (key, average) tuples.
        formatted_results = (
            average_distance_per_passenger
            | 'FormatResults' >> beam.Map(
                # The input to this lambda is a single tuple, let's call it 'item'
                # The tuple will be in the format: (passenger_count, avg_distance)
                lambda item: 
                'Passenger Count: {}, Average Distance: {:.2f}'.format(
                    item[0], item[1]
                )
            )
        )

        # Step 4: Write the results to a new file in the specified GCS bucket.
        formatted_results | 'WriteResults' >> beam.io.WriteToText(OUTPUT_PREFIX)

if __name__ == '__main__':
    run()