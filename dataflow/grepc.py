#!/usr/bin/env python

import apache_beam as beam
import csv

# Define your Google Cloud configuration
# You MUST update these values with your specific project and bucket details.
# The REGION is provided by your qwiklabs environment.
PROJECT = 'qwiklabs-gcp-03-3369e29fea80'
BUCKET = 'qwiklabs-gcp-03-3369e29fea80'
REGION = 'us-east1'

# GCS path for the input file and output directory
INPUT_FILE = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
OUTPUT_PREFIX = 'gs://{0}/avg_distance_by_passengers/output'.format(BUCKET)

def parse_csv_line(line):
    """
    Parses a CSV line and returns a dictionary.
    Assumes the first line is the header.
    """
    try:
        # Menghindari baris header
        if line.startswith('passenger_count'):
            return None
            
        fields = list(csv.reader([line]))[0]
        
        # Kolom yang relevan
        passenger_count = int(fields[0])
        trip_distance = float(fields[1])
        
        # Mengembalikan PCollection dalam bentuk key-value pair
        return (passenger_count, trip_distance)
    except (ValueError, IndexError):
        # Mengabaikan baris yang tidak valid
        return None

def run():
    argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=agregat-data-csv',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/temp/'.format(BUCKET),
      '--region={0}'.format(REGION),
      '--runner=DataflowRunner'
    ]

    p = beam.Pipeline(argv=argv)
    
    (p
     | 'ReadFromCSV' >> beam.io.ReadFromText(INPUT_FILE)
     | 'ParseCSV' >> beam.Map(parse_csv_line)
     | 'FilterNone' >> beam.Filter(lambda x: x is not None)
     | 'GroupByKey' >> beam.GroupByKey()
     | 'CalculateAverage' >> beam.Map(lambda x: (x[0], sum(x[1])/len(x[1])))
     | 'FormatOutput' >> beam.Map(lambda x: f"Jumlah penumpang: {x[0]}, Rata-rata jarak: {x[1]:.2f}")
     | 'WriteResults' >> beam.io.WriteToText(OUTPUT_PREFIX)
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()