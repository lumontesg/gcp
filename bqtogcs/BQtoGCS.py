
""" 
Un pequenio codigo en Python para ejecutarse en Dataflow en Apache Beam. Este 
ejecuta un pipeline para extraer datos desde una tabla de BigQuery y almacenarlos
en GCS en un archivo plano (csv). Esto se da por la consulta SQL que se le pasa como
argumento por la consola de comandos (Cloud Shell) de GCP.
"""

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery


def run(argv=None):
  """Punto de entrada principal, definiciones ejecutores del pipeline"""

  parser = argparse.ArgumentParser()
  #Parseo los argumentos de entrada pasados por consola
  parser.add_argument('--input',
                      dest='input',
                      default='Argumento por defecto que toma',
                      help='Entrada para procesar')
  #Parseo los argumentos de salida pasados por consola
  parser.add_argument('--output',
                      dest='output',
                      default='Bucker de salida por defecto',
                      help='Salida resultante')
  known_args, pipeline_args = parser.parse_known_args(argv)

  #Parametros extendidos que pueden ser agregados en bruto
  """pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=sod-corp-plp-beta',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://test_dataflow_2019_1/staging_location',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://test_dataflow_2019_1/temp_location',
      '--job_name=bigquerytoparquet2test'
  ])"""

  #Creo y guardo las opciones del pipleline en una sesion
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    
    # Defino pasos del pipeline a ejecutar

    # Lectura de una tabla desde BQ
    lines = p | beam.io.Read(beam.io.BigQuerySource (
        query=known_args.input,
        use_standard_sql=True))

    #Escritura en un archivo a GCS
    write = lines | beam.io.WriteToText (
        file_path_prefix= known_args.output, 
        file_name_suffix='.csv')

#Ejecucion del pipeline y escritura en los log's
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()