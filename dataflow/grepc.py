#!/usr/bin/env python

"""
Copyright Google Inc. 2016
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import apache_beam as beam

# Fungsi untuk memparsing baris CSV
def parse_csv(line):
    # Skip header
    if line.startswith('vendor_id'):
        return None  # Lebih eksplisit mengembalikan None
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
    
    input_file = 'gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_1.csv'
    output_prefix = 'gs://{0}/csv_aggregation/output'.format(BUCKET)

    # Membaca data CSV dan melakukan agregasi
    (p 
       | 'GetCSV' >> beam.io.ReadFromText(input_file)
       | 'ParseCSV' >> beam.Map(parse_csv)
       | 'FilterNone' >> beam.Filter(lambda x: x is not None)
       | 'GroupAndSum' >> beam.CombinePerKey(sum)
       | 'FormatOutput' >> beam.Map(lambda key_value: 'Vendor ID: {}, Total Amount: {}'.format(key_value[0], key_value[1]))
       | 'write' >> beam.io.WriteToText(output_prefix)
    )

    p.run()

if __name__ == '__main__':
    run()