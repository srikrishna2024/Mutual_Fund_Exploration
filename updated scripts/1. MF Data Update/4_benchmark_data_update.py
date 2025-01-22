import pandas as pd
import psycopg
from psycopg import sql

def connect_to_db():
    """Establish a database connection."""
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }
    return psycopg.connect(**DB_PARAMS)

def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))
        return cur.fetchone()[0]

def create_table_if_not_exists(conn):
    """Create the benchmark table if it doesn't exist."""
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
    """Read and preprocess the CSV file."""
    data = pd.read_csv(csv_path)
    data = data.fillna(0)  # Fill blank cells with 0
    data.columns = data.columns.str.lower().str.replace(' ', '_')  # Normalize column names
    
    # Explicitly specify the date format to avoid warnings
    data['date'] = pd.to_datetime(data['date'], format='%m/%d/%Y', errors='coerce')
    
    # Remove commas and '%' from numeric fields and convert to numeric types
    numeric_columns = ['price', 'open', 'high', 'low', 'vol', 'change_percent']
    for column in numeric_columns:
        if column in data.columns:
            data[column] = (
                data[column]
                .astype(str)
                .str.replace(',', '')  # Remove commas
                .str.replace('%', '')  # Remove percentage signs
                .astype(float)  # Convert to float
            )
        else:
            print(f"Warning: Column '{column}' not found in the CSV. Filling with 0.")
            data[column] = 0  # Add missing column with default value 0
    
    return data

def load_initial_data(conn, data):
    """Perform an initial data load."""
    with conn.cursor() as cur:
        for _, row in data.iterrows():
            cur.execute("""
                INSERT INTO benchmark (date, price, open, high, low, vol, change_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        conn.commit()

def incremental_update(conn, data):
    """Perform an incremental update."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM benchmark")
        last_date = cur.fetchone()[0]
        new_data = data[data['date'] > last_date] if last_date else data
        for _, row in new_data.iterrows():
            cur.execute("""
                INSERT INTO benchmark (date, price, open, high, low, vol, change_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        conn.commit()

def refresh_data(conn, data):
    """Refresh the table by cleaning up and reloading all data."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM benchmark")
        conn.commit()
    load_initial_data(conn, data)

def main():
    csv_path = r"C:\Users\skchaitanya\Downloads\NIFTY_Updated_Dates.csv"  # Replace with your CSV file path
    table_name = "benchmark"

    print("Options:\n1. Initial Data Load\n2. Incremental Update\n3. Refresh Data")
    choice = input("Enter your choice (1/2/3): ")

    data = preprocess_csv(csv_path)

    with connect_to_db() as conn:
        if not check_table_exists(conn, table_name):
            print(f"Table '{table_name}' does not exist. Creating the table...")
            create_table_if_not_exists(conn)
        
        if choice == "1":
            load_initial_data(conn, data)
            print("Initial data load complete.")
        elif choice == "2":
            incremental_update(conn, data)
            print("Incremental update complete.")
        elif choice == "3":
            refresh_data(conn, data)
            print("Data refresh complete.")
        else:
            print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()
