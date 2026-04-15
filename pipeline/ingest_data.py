#!/usr/bin/env python
# coding: utf-8


import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm




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


@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres username')
@click.option('--pg-pass', default='root', show_default=True, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database name')
@click.option('--pg-port', default=5432, show_default=True, help='Postgres port')
@click.option('--year', default=2021, show_default=True, help='Year for the taxi data file')
@click.option('--month', default=1, show_default=True, help='Month for the taxi data file')
@click.option('--chunk-size', default=100000, show_default=True, help='CSV chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', show_default=True, help='Target Postgres table name')
def run(pg_user, pg_pass, pg_host, pg_db, pg_port, year, month, chunk_size, target_table):
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    df_iter = pd.read_csv(
        prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size,
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

if __name__ == '__main__':
    run()




