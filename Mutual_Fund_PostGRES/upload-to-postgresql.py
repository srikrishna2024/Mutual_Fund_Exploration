import os
import pandas as pd
import psycopg
from psycopg.rows import dict_row
import sys
import argparse
from datetime import datetime
import importlib.util

def check_dependencies():
    """Check if required dependencies are installed."""
    missing_deps = []
    
    # Check for psycopg
    if importlib.util.find_spec("psycopg") is None:
        missing_deps.append("psycopg")
    
    # Check for pandas
    if importlib.util.find_spec("pandas") is None:
        missing_deps.append("pandas")
    
    if missing_deps:
        print("Missing required dependencies:", ", ".join(missing_deps))
        print("\nPlease install them using pip:")
        print(f"pip install {' '.join(missing_deps)}")
        return False
    return True

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

class DatabaseHandler:
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
    
    def connect(self):
        """Establish database connection with error handling."""
        try:
            self.conn = psycopg.connect(
                **self.db_params,
                row_factory=dict_row,
                autocommit=False
            )
            return True
        except Exception as e:
            print(f"Database connection error: {str(e)}")
            return False
    
    def create_table(self):
        """Create the mutual funds NAV table if it doesn't exist."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS mutual_fund_nav (
                    scheme_code VARCHAR(20),
                    date DATE,
                    net_asset_value DECIMAL(20, 4),
                    scheme_name VARCHAR(255),
                    PRIMARY KEY (scheme_code, date)
                )
                """)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            self.conn.rollback()
            return False
    
    def insert_data(self, data, batch_size=1000):
        """Insert data into the database with batch processing."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                # Process data in batches
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    cur.executemany("""
                        INSERT INTO mutual_fund_nav 
                            (scheme_code, date, net_asset_value, scheme_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (scheme_code, date) DO UPDATE
                        SET net_asset_value = EXCLUDED.net_asset_value
                        """, batch)
                    
                    # Print progress for large datasets
                    if len(data) > batch_size:
                        progress = min(i + batch_size, len(data))
                        print(f"Processed {progress}/{len(data)} records", end='\r')
                
            self.conn.commit()
            if len(data) > batch_size:
                print()  # New line after progress
            return True
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            self.conn.rollback()
            return False
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

def process_csv_file(file_path, db_handler):
    """Process a single CSV file and upload its data to the database."""
    try:
        print(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Prepare data for insertion
        data = df[['Scheme Code', 'date', 'Net Asset Value', 'Scheme Name']].values.tolist()
        
        print(f"Inserting {len(data)} records from {file_path}")
        if db_handler.insert_data(data):
            print(f"Successfully processed {file_path}")
            return True
        return False
    
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return False

def main(csv_directory):
    # Check dependencies first
    if not check_dependencies():
        return

    print(f"Current working directory: {os.getcwd()}")
    print(f"Looking for CSV files in: {csv_directory}")

    # Create database handler
    db = DatabaseHandler(DB_PARAMS)
    
    try:
        # Connect to database
        if not db.connect():
            return
        print("Connected to the database successfully.")

        # Create table if it doesn't exist
        if not db.create_table():
            return
        print("Table 'mutual_fund_nav' created or already exists.")

        # Check if directory exists
        if not os.path.exists(csv_directory):
            raise FileNotFoundError(f"The directory '{csv_directory}' does not exist.")

        # Get list of CSV files
        csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
        if not csv_files:
            print(f"No CSV files found in {csv_directory}")
            return

        # Process each CSV file
        total_files = len(csv_files)
        successful_files = 0

        for i, file_name in enumerate(csv_files, 1):
            file_path = os.path.join(csv_directory, file_name)
            print(f"\nProcessing file {i} of {total_files}: {file_name}")
            
            if process_csv_file(file_path, db):
                successful_files += 1

        # Print summary
        print(f"\nProcessing completed:")
        print(f"Total files: {total_files}")
        print(f"Successfully processed: {successful_files}")
        print(f"Failed: {total_files - successful_files}")

    except Exception as error:
        print(f"Error while processing files: {error}", file=sys.stderr)

    finally:
        db.close()
        print("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process mutual fund data CSV files and upload to PostgreSQL.")
    parser.add_argument("--directory", type=str, default="mutual_fund_data",
                      help="Directory containing CSV files")
    args = parser.parse_args()

    main(args.directory)
