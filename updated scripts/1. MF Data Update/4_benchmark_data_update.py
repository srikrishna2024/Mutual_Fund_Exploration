import pandas as pd
import psycopg
from datetime import datetime
from psycopg import sql

def connect_to_db():
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }
    return psycopg.connect(**DB_PARAMS)
def get_most_recent_date(conn):
    """Get the most recent date from the benchmark table."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM benchmark")
        most_recent_date = cur.fetchone()[0]
        return pd.to_datetime(most_recent_date) if most_recent_date else None

def check_table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        return cur.fetchone()[0]

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                price NUMERIC DEFAULT 0,
                open NUMERIC DEFAULT 0,
                high NUMERIC DEFAULT 0,
                low NUMERIC DEFAULT 0,
                vol NUMERIC DEFAULT 0,
                change_percent NUMERIC DEFAULT 0
            )
        """)
        conn.commit()

def preprocess_csv(csv_path):
    data = pd.read_csv(csv_path)
    data = data.fillna(0)
    data.columns = data.columns.str.lower().str.replace(' ', '_')
    
    date_formats = ['%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y', '%Y-%m-%d']
    
    for date_format in date_formats:
        try:
            data['date'] = pd.to_datetime(data['date'], format=date_format)
            if not data['date'].isna().all():
                break
        except:
            continue
    
    if data['date'].isna().all():
        raise ValueError("Could not parse dates. Check the date format in your CSV file.")
    
    if data['date'].isna().any():
        print(f"Warning: Dropping {data['date'].isna().sum()} rows with invalid dates.")
        data = data.dropna(subset=['date'])

    numeric_columns = ['price', 'open', 'high', 'low', 'vol', 'change_percent']
    for column in numeric_columns:
        if column in data.columns:
            data[column] = (data[column].astype(str)
                          .str.replace(',', '')
                          .str.replace('%', '')
                          .astype(float))
        else:
            print(f"Warning: Column '{column}' not found. Filling with 0.")
            data[column] = 0
    
    return data

def load_initial_data(conn, data):
    """
    Loads initial benchmark data into the database.
    This function inserts rows into the 'benchmark' table from the provided DataFrame `data`.
    It first checks if there are existing records in the database. If records exist, it only
    inserts new data if the earliest date in the DataFrame is more recent than the most recent
    date in the database. If no records exist, it proceeds with the initial load.
    Parameters:
    conn (psycopg2.extensions.connection): The connection object to the PostgreSQL database.
    data (pandas.DataFrame): The DataFrame containing the benchmark data to be inserted. It must
                             have columns: 'date', 'price', 'open', 'high', 'low', 'vol', and 'change_percent'.
    Returns:
    int: The number of rows inserted into the database. Returns -1 if records already exist and no new data was inserted.
    """
    most_recent_date = get_most_recent_date(conn)
    
    if most_recent_date is not None:
        oldest_csv_date = data['date'].min()
        if oldest_csv_date > most_recent_date:
            rows_inserted = 0
            with conn.cursor() as cur:
                for _, row in data.iterrows():
                    cur.execute("""
                        INSERT INTO benchmark (date, price, open, high, low, vol, change_percent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['date'].strftime('%Y-%m-%d'),
                        row['price'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['vol'],
                        row['change_percent']
                    ))
                    rows_inserted += 1
                conn.commit()
            return rows_inserted
        else:
            return -1  # Indicates that records already exist
    else:
        # No existing records, proceed with initial load
        rows_inserted = 0
        with conn.cursor() as cur:
            for _, row in data.iterrows():
                cur.execute("""
                    INSERT INTO benchmark (date, price, open, high, low, vol, change_percent)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['date'].strftime('%Y-%m-%d'),
                    row['price'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['vol'],
                    row['change_percent']
                ))
                rows_inserted += 1
            conn.commit()
        return rows_inserted

def incremental_update(conn, data):
    most_recent_date = get_most_recent_date(conn)
    
    if most_recent_date is not None:
        oldest_csv_date = data['date'].min()
        if oldest_csv_date > most_recent_date:
            rows_inserted = 0
            new_data = data[data['date'] > most_recent_date]
            
            with conn.cursor() as cur:
                for _, row in new_data.iterrows():
                    cur.execute("""
                        INSERT INTO benchmark (date, price, open, high, low, vol, change_percent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['date'].strftime('%Y-%m-%d'),
                        row['price'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['vol'],
                        row['change_percent']
                    ))
                    rows_inserted += 1
                conn.commit()
            return rows_inserted, len(new_data)
        else:
            return -1, 0  # Indicates that records already exist
    else:
        # No existing records, proceed with all data
        return load_initial_data(conn, data), len(data)

def refresh_data(conn):
    """Clean up all data from the benchmark table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM benchmark")
        deleted_count = cur.fetchone()[0]
        cur.execute("DELETE FROM benchmark")
        conn.commit()
    return deleted_count

def main():
    csv_path = r"C:\Users\skchaitanya\Downloads\NIFTY.csv"
    table_name = "benchmark"

    try:
        data = preprocess_csv(csv_path)
        print(f"Date range in data: {data['date'].min().strftime('%d/%m/%Y')} to {data['date'].max().strftime('%d/%m/%Y')}")
        total_rows = len(data)

        print("Options:\n1. Initial Data Load\n2. Incremental Update\n3. Refresh Data")
        choice = input("Enter your choice (1/2/3): ")

        with connect_to_db() as conn:
            if not check_table_exists(conn, table_name):
                print(f"Creating table '{table_name}'...")
                create_table_if_not_exists(conn)
            
            if choice == "1":
                rows_inserted = load_initial_data(conn, data)
                if rows_inserted == -1:
                    print("Records already exist in the table. No new records inserted.")
                else:
                    print(f"Inserted {rows_inserted} of {total_rows} records.")
            elif choice == "2":
                rows_inserted, new_data_count = incremental_update(conn, data)
                if rows_inserted == -1:
                    print("Records already exist in the table. No new records inserted.")
                else:
                    print(f"Inserted {rows_inserted} of {new_data_count} new records.")
            elif choice == "3":
                deleted_count = refresh_data(conn)
                print(f"{deleted_count} records deleted")
            else:
                print("Invalid choice. Exiting.")
    
    except ValueError as e:
        print(f"Error: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    main()