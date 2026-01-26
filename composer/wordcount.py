import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # Define pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',  # Use 'DataflowRunner' for Google Cloud Dataflow or 'DirectRunner' for local execution
        project='ordinal-rig-485316-r2',
        temp_location='gs://gcs-bucket-curso-05-prueba/temp',
        region='us-central1'
    )

    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            # Read input text file
            | 'Leer Archivo' >> beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
            # Split lines into words
            | 'Separar Palabras' >> beam.FlatMap(lambda line: line.split())
            # Pair each word with 1
            | 'Contar Palabras' >> beam.combiners.Count.PerElement()
            # Count occurrences of each word
            | 'Guardar Resultados' >> beam.io.WriteToText('gs://gcs-bucket-curso-05-prueba/output/wordcounts')
        )

if __name__ == '__main__':
    run()