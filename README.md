# world-systems-analyzer
A web app built with React + Django + Postgres to explore Thomas Picketty's World Inequality Database

## Development process:

1. Downloaded full dataset from https://wid.world/data/
2. Pulled official Postgres Docker image, copied all CSVs to container
3. Ran load_data_postgres.py, which creates a singular table in Postgres (partitioned on country code and indexed on year number) and uploads the contents of each of each data file from https://wid.world/data/ into it, along with a reference table explaining all country codes
4. Ran load_metadata_postgres.py, which creates a singular table that contains full definitions for each variable present in the data files uploaded in the previous step