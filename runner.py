from google.cloud import bigquery

def query_runner():
    client=bigquery.Client()
    job=client.query("""
    SELECT
    zipcode 
    FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`
     where population>100000
    """)

    result=job.result()


    for row in result:
        print(row[0])

query_runner()
