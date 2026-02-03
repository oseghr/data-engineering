#!/usr/bin/env python
# coding: utf-8

#install package.
# get_ipython().system('uv add sqlalchemy psycopg2-binary tqdm click pandas')

#import dependencies
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--year', default=2021, type=int, help='Data year')
@click.option('--month', default=1, type=int, help='Data month')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')




#iterating over the data in batches because the large dataset will take a while to load.
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):
    #update the datatypes for the dataset
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]


    #define parameters
    # pg_user = "root"
    # pg_pass = "root"
    # pg_host = "localhost"
    # pg_port = 5432
    # pg_db = "ny_taxi"
    # year = 2021
    # month = 1
    # target_table = "yellow_taxi_data"
    chunksize=100000

    #create the sql database connection
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    #read data from url
    df_iter = pd.read_csv(
        url, 
        dtype=dtype, 
        parse_dates=parse_dates, 
        iterator=True,
        chunksize=chunksize,
    )

    first = True 

    for df_chunk in tqdm(df_iter):
        #create the sql database connection
        engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
        if first:
            df_chunk.head(0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='append',
                chunksize=chunksize,
            )

            first = False

        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append',
            chunksize=chunksize,
        )
        print(f'Inserted another {len(df_chunk)} rows to table {target_table}.')

if __name__ == '__main__':
    run()