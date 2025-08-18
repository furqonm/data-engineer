#!/usr/bin/env python

import apache_beam as beam

# Fungsi untuk memparsing baris CSV
def parse_csv(line):
    # Skip header
    if line.startswith('vendor_id'):
        return
    # Pecah baris berdasarkan koma
    parts = line.split(',')
    # Dapatkan vendor_id (indeks 0) dan total_amount (indeks 14)
    vendor_id = parts[0]
    total_amount = float(parts[14])
    return (vendor_id, total_amount)

PROJECT = 'qwiklabs-gcp-04-761ec0adb4a4'
BUCKET = 'qwiklabs-gcp-04-761ec0adb4a4'
REGION = 'us-east4'

def run():
    argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=examplejob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--region={0}'.format(REGION),
      '--worker_machine_type=e2-standard-2',
      '--runner=DataflowRunner'
    ]

    p = beam.Pipeline(argv=argv)
    
    # Ubah input path ke lokasi file CSV Anda di Google Cloud Storage
    input_file = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
    output_prefix = 'gs://{0}/csv_aggregation/output'.format(BUCKET)

    # Membaca data CSV dan melakukan agregasi
    (p 
       | 'GetCSV' >> beam.io.ReadFromText(input_file)
       | 'ParseCSV' >> beam.Map(parse_csv)
       | 'GroupAndSum' >> beam.CombinePerKey(sum)
       | 'FormatOutput' >> beam.Map(lambda (key, value): 'Vendor ID: {}, Total Amount: {}'.format(key, value))
       | 'write' >> beam.io.WriteToText(output_prefix)
    )

    p.run()

if __name__ == '__main__':
    run()