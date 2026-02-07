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
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Load taxi zones FIRST (moved outside the loop)
    try:
        df_zone = pd.read_csv(r'D:\Data\Data-ZoomCamp\New_DE26\DE_ZoomCamp26\Week_1\taxi_zone_lookup.csv')
        print("Success! Loaded zones:", len(df_zone), "rows")
        df_zone.to_sql('taxi_zones', con=engine, if_exists='replace', index=False)
        print("Taxi zones loaded into database")
    except FileNotFoundError:
        print("Zones file not found! Check the path.")
    except Exception as e:
        print(f"Error loading zones: {e}")

    # Load CSV data (original code - 2021 data)
    print("\n=== Loading 2021 CSV data ===")
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True
    for df_chunk in tqdm(df_iter, desc="Loading 2021 data"):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )

    # Load parquet files and INSERT INTO DATABASE
    print("\n=== Loading 2025 parquet files ===")
    parquet_files = [
        (r'D:\Data\Data-ZoomCamp\New_DE26\DE_ZoomCamp26\Week_1\yellow_tripdata_2025-11.parquet', 'yellow_taxi_2025'),
        (r'D:\Data\Data-ZoomCamp\New_DE26\DE_ZoomCamp26\Week_1\green_tripdata_2025-11.parquet', 'green_taxi_2025')
    ]

    for parquet_file, table_name in parquet_files:
        try:
            # Read parquet
            df = pd.read_parquet(parquet_file)
            print(f"Loaded {parquet_file}: {len(df)} rows")
            
            # Optional: Save as CSV
            csv_file = parquet_file.replace('.parquet', '.csv')
            df.to_csv(csv_file, index=False)
            print(f"Saved as CSV: {csv_file}")
            
            # Load into database in chunks
            print(f"Loading {table_name} into database...")
            for i in tqdm(range(0, len(df), chunksize), desc=table_name):
                chunk = df.iloc[i:i+chunksize]
                if i == 0:
                    chunk.to_sql(table_name, con=engine, if_exists='replace', index=False)
                else:
                    chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
            
            print(f"✓ {table_name} loaded successfully!")
            
        except Exception as e:
            print(f"Error with {parquet_file}: {e}")

    print("\n=== All data loaded! ===")

if __name__ == '__main__':
    run()