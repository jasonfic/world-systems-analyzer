import argparse
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# === CONFIG ===
load_dotenv()
parser = argparse.ArgumentParser()
parser.add_argument('path', type=str, help='Path of folder containing CSVs to import to Postgres')
args = parser.parse_args()
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'wid',
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PW']
}
TABLE_NAME = 'variable_metadata'

# === COLUMNS ===
METADATA_COLUMNS = [
    'country', 'variable', 'age', 'pop', 'countryname', 'shortname',
    'simpledes', 'technicaldes', 'shorttype', 'longtype', 'shortpop',
    'longpop', 'shortage', 'longage', 'unit', 'source', 'method'
]

def get_largest_metadata_files(directory, top_n=5):
    metadata_files = [
        os.path.join(directory, f) for f in os.listdir(directory)
        if f.startswith('WID_metadata_') and f.endswith('.csv')
    ]
    metadata_files.sort(key=lambda f: os.path.getsize(f), reverse=True)
    return metadata_files[:top_n]

def create_metadata_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        cur.execute(f"""
            CREATE TABLE {TABLE_NAME} (
                country TEXT,
                variable TEXT,
                age TEXT,
                pop TEXT,
                countryname TEXT,
                shortname TEXT,
                simpledes TEXT,
                technicaldes TEXT,
                shorttype TEXT,
                longtype TEXT,
                shortpop TEXT,
                longpop TEXT,
                shortage TEXT,
                longage TEXT,
                unit TEXT,
                source TEXT,
                method TEXT
            );
        """)
        conn.commit()

def insert_metadata(conn, df):
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(sql.SQL(f"""
                INSERT INTO {TABLE_NAME} ({','.join(METADATA_COLUMNS)})
                VALUES ({','.join(['%s'] * len(METADATA_COLUMNS))})
            """), tuple(row[col] for col in METADATA_COLUMNS))
        conn.commit()

def deduplicate_metadata(conn):
    with conn.cursor() as cur:
        dedup_query = f"""
            DELETE FROM {TABLE_NAME} a
            USING {TABLE_NAME} b
            WHERE a.ctid > b.ctid
              AND a.variable = b.variable
              AND a.age = b.age
              AND a.pop = b.pop;
        """
        cur.execute(dedup_query)
        conn.commit()

def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    print("Creating metadata table...")
    create_metadata_table(conn)

    print("Loading largest metadata files...")
    top_files = get_largest_metadata_files(args.path)
    print(f"Top 5 largest metadata files: {top_files}")

    for file in top_files:
        print(f"Importing {file}...")
        df = pd.read_csv(file, sep=';', usecols=METADATA_COLUMNS, dtype=str)
        df.fillna('', inplace=True)  # Replace NaN with empty string
        insert_metadata(conn, df)

    print("Deduplicating metadata entries...")
    deduplicate_metadata(conn)

    conn.close()
    print("✅ Done: Data loaded and deduplicated.")

if __name__ == "__main__":
    main()
