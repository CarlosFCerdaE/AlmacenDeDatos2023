import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

# Especifica las rutas a los archivos TSV a combinar
input_file1_nameBasics = 'gs://bucketimdb/nameBasics.tsv'
input_file2_titleBasics = 'gs://bucketimdb/titleBasics.tsv'
input_file3_titlePrincipals = 'gs://bucketimdb/titlePrincipals.tsv'
input_file4_titleRatings = 'gs://bucketimdb/titleRatings.tsv'

# Especifica las tablas de BigQuery a cargar los datos
output_table_dimTitleCast = 'data.dimTitleCast'
output_table_dimTitles = 'data.dimTitles'
output_table_factReviews = 'data.factReviews'

# Define el esquema de la tabla de salida en formato JSON

output_schema_dimTitleCast = {
    'fields': [
        {'name': 'idTitleCast', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'primaryName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birthYear', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'deathYear', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'primaryProfession', 'type': 'STRING', 'mode': 'REPEATED'},
        {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'job', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'characters', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}

output_schema_dimTitles = {
    'fields': [
        {'name': 'idTitles', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'primaryTitle', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'originalTitle', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'isAdult', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'genres', 'type': 'STRING', 'mode': 'REPEATED'}
    ]
}


output_schema_factReviews = {
    'fields': [
        {'name': 'idReview', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'startYear', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'endYear', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'runtimeMinutes', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'averageRating', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'numVotes', 'type': 'STRING', 'mode': 'REPEATED'},
        {'name': 'idTitleCast', 'type': 'STRING', 'mode': 'REPEATED'},
        {'name': 'idTitles', 'type': 'STRING', 'mode': 'REPEATED'}
    ]
}

def fill_dim_titles():
    # Define las opciones del pipeline de Apache Beam
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Lee el archivo TSV con Apache Beam
    lines = (p 
             | 'Read TSV' >> beam.io.ReadFromText(input_file2_titleBasics) 
             | 'Limit to 50000 lines' >> beam.combiners.Sample.FixedSizeGlobally(50000)
             )
    # Parsea cada línea del archivo TSV
    parsed_lines = (lines
                    | 'Parse TSV' >> beam.Map(lambda line: line.split('\t'))
                    | 'Extract Fields' >> beam.Map(lambda fields: {
                        'idTitles': fields[0],
                        'type': fields[1],
                        'primaryTitle': fields[2],
                        'originalTitle': fields[3],
                        'isAdult': True if fields[4] == '1' else False,
                        'genres': [] if fields[8] == '\\N' else fields[8].split(',')
                    })
                    )


    # Escribe los resultados a BigQuery
    _ = (parsed_lines
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             output_table_dimTitles,
             schema=parse_table_schema_from_json(json.dumps(output_schema_dimTitles)),
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         ))

    # Ejecuta el pipeline
    result = p.run()
    result.wait_until_finish()

    return 'Tabla dimTitles actualizada con éxito'


class Combine(beam.DoFn):
    def process(self, element):
        nameid = element[0]
        basics = list(element[1]['basics'])
        principals = list(element[1]['principals'])
        for basic in basics:
            primaryName, birthYear, deathYear, primaryProfession = basic
            for principal in principals:
                category, job, characters = principal
                yield (nameid, (primaryName, birthYear, deathYear, primaryProfession, category, job, characters))


                # Key: nconst, Value: (primaryName, birthYear,deathYear,primaryProfession)
                # Key: nconst, Value: (catergory, job,characters)

def fill_dim_titleCast():
    # Define las opciones del pipeline de Apache Beam
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Lee el archivo TSV con Apache Beam

    linesBasics = (p 
                | 'Read TSV Basics' >> beam.io.ReadFromText(input_file1_nameBasics)
                | 'Limit to 50000 lines' >> beam.combiners.Sample.FixedSizeGlobally(50000)
                | 'Parse Basics' >> beam.Map(lambda line: line.split('\t'))
                | 'Filter Basics' >> beam.Filter(lambda fields: len(fields) == 6)
                | 'Map Basics' >> beam.Map(lambda fields: (fields[0], (fields[1], fields[2],fields[3],fields[4],fields[5])))) # Key: nconst, Value: (primaryName, birthYear,deathYear,primaryProfession)

    linesPrincipals = (p 
                    | 'Read TSV Principals' >> beam.io.ReadFromText(input_file3_titlePrincipals)
                    | 'Limit to 50000 lines' >> beam.combiners.Sample.FixedSizeGlobally(50000)
                    | 'Parse Principals' >> beam.Map(lambda line: line.split('\t'))
                    | 'Filter Principals' >> beam.Filter(lambda fields: len(fields) == 6)
                    | 'Map Principals' >> beam.Map(lambda fields: (fields[2], (fields[3], fields[4],fields[5])))) # Key: nconst, Value: (catergory, job,characters)

    combined = ({'basics': linesBasics, 'principals': linesPrincipals} 
                | beam.CoGroupByKey()
                | 'Combine' >> beam.ParDo(Combine()))



    # Parsea cada línea del archivo TSV
    parsed_lines = (combined
                    | 'Parse TSV' >> beam.Map(lambda line: line.split('\t'))
                    | 'Extract Fields' >> beam.Map(lambda fields: {
                        'nconst': fields[0],
                        'primaryName': fields[1],
                        'birthYear': fields[2],
                        'deathYear': fields[3],
                        'primaryProfession': [] if fields[4] == '\\N' else fields[4].split(','),
                        'category': fields[5],
                        'job': fields[6],
                        'characters': fields[7]
                    })
                    )
    
    


    # Escribe los resultados a BigQuery
    _ = (parsed_lines
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             output_table_dimTitleCast,
             schema=parse_table_schema_from_json(json.dumps(output_schema_dimTitleCast)),
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         ))

    # Ejecuta el pipeline
    result = p.run()
    result.wait_until_finish()

    return 'Tabla dimTitleCast actualizada con éxito'

def run(argv=None):
    print(fill_dim_titles())
    print(fill_dim_titleCast())


    

if __name__ == '__main__':
    run()