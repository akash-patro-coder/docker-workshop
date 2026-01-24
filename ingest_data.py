#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def main():
    # ---------------------------
    # CONFIGURATION
    # ---------------------------
    pg_user = "postgres"       # use postgres to avoid permission issues
    pg_pass = "postgres"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "ny_taxi"

    year = 2021
    month = 1
    table_name = "yellow_taxi_data"
    chunksize = 100_000

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    print(f"Downloading data from: {url}")

    # ---------------------------
    # DATA TYPES
    # ---------------------------
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
        "congestion_surcharge": "float64",
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]

    # ---------------------------
    # DATABASE CONNECTION
    # ---------------------------
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # ---------------------------
    # LOAD DATA IN CHUNKS
    # ---------------------------
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        chunksize=chunksize,
    )

    first_chunk = True

    for df_chunk in tqdm(df_iter, desc="Loading data to Postgres"):
        if first_chunk:
            # Create table
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first_chunk = False

        # Insert data
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

    print("✅ Data load completed successfully")


if __name__ == "__main__":
    main()
