import argparse
from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd

# === CONFIGURATION ===
load_dotenv()
parser = argparse.ArgumentParser()
parser.add_argument('path', type=str, help='Path of folder containing CSVs to import to Postgres')
args = parser.parse_args()

CSV_PATH = f"{args.path}/WID_countries.csv"  # Path to your WID_countries.csv
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'wid',
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PW']
}

# === SQL TEMPLATE DEFINITIONS ===
CREATE_MAIN_TABLE_SQL = """
DROP TABLE IF EXISTS global_data CASCADE;
CREATE TABLE global_data (
    country TEXT NOT NULL,
    variable TEXT NOT NULL,
    percentile TEXT NOT NULL,
    year INTEGER NOT NULL,
    value NUMERIC,
    age TEXT,
    pop TEXT
) PARTITION BY LIST (country);
"""

PARTITION_SQL_TEMPLATE = """
CREATE TABLE global_data_{code} PARTITION OF global_data
FOR VALUES IN ('{file_code}');
"""

INDEX_SQL_TEMPLATE = """
CREATE INDEX global_data_{code}_year_idx ON global_data_{code} (year);
CLUSTER global_data_{code} USING global_data_{code}_year_idx;
"""

COPY_CSV_TEMPLATE = """
COPY global_data_{code} FROM '/data/WID_data_{file_code}.csv' DELIMITER ';' CSV HEADER;
"""

# === MAIN EXECUTION ===
def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating main partitioned table...")
    cur.execute(CREATE_MAIN_TABLE_SQL)

    # Load country codes from CSV
    print(f"Reading country codes from {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH, sep=';')
    alpha2_codes = df['alpha2'].dropna().unique()

    print(f"Found {len(alpha2_codes)} country codes.")

    # Iterate through country codes to create partitions and indexes
    for code in alpha2_codes:
        table_code = code.lower().replace('-', '_')  # handle region codes like US-WA
        create_partition_sql = PARTITION_SQL_TEMPLATE.format(code=table_code, file_code=code)
        create_index_sql = INDEX_SQL_TEMPLATE.format(code=table_code)
        copy_data_cmd = COPY_CSV_TEMPLATE.format(code=table_code, file_code=code)

        try:
            print(f"Creating partition for country: {code}...")
            cur.execute(create_partition_sql)
            cur.execute(create_index_sql)
            cur.execute(copy_data_cmd)
        except Exception as e:
            print(f"Error creating partition for {code}: {e}")

    # Cleanup
    cur.close()
    conn.close()
    print("All partitions and indexes created successfully.")

if __name__ == "__main__":
    main()
