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

# === COLUMNS ===
METADATA_COLUMNS = [
    'country', 'variable', 'age', 'pop', 'countryname', 'shortname',
    'simpledes', 'technicaldes', 'shorttype', 'longtype', 'shortpop',
    'longpop', 'shortage', 'longage', 'unit', 'source', 'method'
]

def load_all_metadata_to_global_table(conn, table_name, directory):
    with conn.cursor() as cur:
        # Drop and recreate the global_metadata table
        cur.execute(f"DROP TABLE IF EXISTS {table_name}_raw;")
        cur.execute(f"""
            CREATE TABLE {table_name}_raw (
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

    # Process all metadata CSVs
    for filename in os.listdir(directory):
        if filename.startswith('WID_metadata_') and filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            print(f"Importing: {filepath}")
            df = pd.read_csv(filepath, sep=';', usecols=METADATA_COLUMNS, dtype=str)
            df.fillna('', inplace=True)

            # Insert into Postgres
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute(sql.SQL(f"""
                        INSERT INTO {table_name}_raw ({','.join(METADATA_COLUMNS)})
                        VALUES ({','.join(['%s'] * len(METADATA_COLUMNS))})
                    """), tuple(row[col] for col in METADATA_COLUMNS))
                conn.commit()

def create_global_data_units(conn, table_name, join_table_name):
    with conn.cursor() as cur:
        print("Creating global_data table with units included...")
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT
                d.country,
                d.variable,
                d.age,
                d.pop,
                m.unit,
                m.source,
                m.method
            FROM
                global_data d
            LEFT JOIN
                {join_table_name} m
            ON
                d.country = m.country AND
                d.variable = m.variable AND
                d.age = m.age AND
                d.pop = m.pop;
        """)
        conn.commit()
        print("✅ global_data_units created.")

def create_deduplicated_variable_metadata(conn, table_name, old_table_name):
    with conn.cursor() as cur:
        print("Creating deduplicated metadata table...")
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        # All columns except 'unit'
        nondup_cols = [col for col in METADATA_COLUMNS if col not in ['country', 'countryname', 'unit', 'source', 'method']]

        cur.execute(sql.SQL(f"""
            CREATE TABLE {table_name} AS
            SELECT DISTINCT {', '.join(nondup_cols)}
            FROM {old_table_name};
        """))
        conn.commit()
        print("✅ metadata table created with deduplicated rows.")

        
        
def main():
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    tname = "variable_metadata"
    print("Loading metadata into raw metadata table...")
    load_all_metadata_to_global_table(conn, tname, args.path)
    
    print("Joining metadata with global_data...")
    create_global_data_units(conn, "wid_global_data", f"{tname}_raw")
    
    print("Creating deduplicated variable_metadata table...")
    create_deduplicated_variable_metadata(conn, tname, f"{tname}_raw")

    conn.close()
    print("✅ Done: Data loaded and deduplicated.")

if __name__ == "__main__":
    main()
