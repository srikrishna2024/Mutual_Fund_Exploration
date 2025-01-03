import os
import pandas as pd
import psycopg
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

def connect_to_db():
    """Establish and return a database connection."""
    try:
        conn = psycopg.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def debug_csv_import(csv_file):
    """
    Comprehensive CSV debugging function
    Returns a cleaned DataFrame if successful, otherwise None
    """
    try:
        print("\n--- CSV IMPORT DEBUGGING ---")
        print(f"Attempting to read file: {csv_file}")
        
        # Try different parsing strategies
        parsing_strategies = [
            {'dayfirst': False},
            {'dayfirst': True},
            {'format': '%Y-%m-%d'},
            {'format': '%d-%m-%Y'},
            {'format': '%m/%d/%Y'},
            {'format': '%d-%b-%Y'}
        ]
        
        for strategy in parsing_strategies:
            print(f"\nTrying parsing strategy: {strategy}")
            try:
                # Read CSV
                df = pd.read_csv(csv_file)
                print("Raw CSV Columns:", list(df.columns))
                print("Sample First Row:")
                print(df.head(1))
                
                # Check date column variations
                date_columns = [col for col in df.columns if 'date' in col.lower()]
                
                for date_col in date_columns:
                    print(f"\nExamining column: {date_col}")
                    try:
                        # Attempt date parsing
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce', **strategy)
                        
                        # Print parsing results
                        print(f"Successful parsing of {date_col} with strategy {strategy}")
                        print("Sample parsed dates:")
                        print(df[date_col].head())
                        
                        # Check for any parsing issues
                        null_count = df[date_col].isnull().sum()
                        print(f"Null values after parsing: {null_count}")
                        
                        # Return DataFrame if parsing is successful
                        return df
                        
                    except Exception as date_parse_error:
                        print(f"Date parsing error for {date_col}: {date_parse_error}")
                
            except Exception as read_error:
                print(f"Error with parsing strategy {strategy}: {read_error}")
        
        # If no strategy works
        return None
    
    except Exception as e:
        print(f"Comprehensive debugging failed: {e}")
        return None

def table_exists(conn, table_name):
    """Check if a table exists in the database."""
    query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = %s
    );
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return cur.fetchone()[0]

def print_table_columns(conn, table_name):
    """Print existing columns in the table."""
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        columns = cur.fetchall()
        print(f"Existing columns in {table_name}:")
        for column in columns:
            print(column[0])

def get_most_recent_date(conn):
    """Get the most recent date from the `benchmark_index` table."""
    query = "SELECT MAX(date) FROM benchmark_index;"
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]
        return result

def create_table(conn, df_columns):
    """Create the `benchmark_index` table with dynamic column names."""
    columns_definitions = ", ".join([
        "id SERIAL PRIMARY KEY",
        "indexname TEXT",
        "date DATE", 
        "price NUMERIC"
    ])
    
    query = f"""
    CREATE TABLE benchmark_index (
        {columns_definitions}
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()
    print("Table `benchmark_index` created.")

def prepare_dataframe(df):
    """
    Prepare DataFrame for insertion into benchmark_index table
    Ensures correct columns and data types
    """
    # Normalize column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    
    # Rename columns to match table structure if needed
    column_mapping = {
        'index': 'indexname',
        'index_name': 'indexname',
        'date': 'date',
        'value': 'price',
        'closing_value': 'price'
    }
    
    # Rename columns based on mapping
    df = df.rename(columns={old: new for old, new in column_mapping.items() if old in df.columns})
    
    # Ensure required columns exist
    required_columns = ['indexname', 'date', 'price']
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Required column '{col}' is missing from the DataFrame")
            return None
    
    # Comprehensive date parsing and type conversion
    # Try multiple strategies to parse the date column
    date_formats = [
        None,  # Let pandas auto-detect
        '%Y-%m-%d', 
        '%d-%m-%Y', 
        '%m/%d/%Y', 
        '%d/%m/%Y',
        '%Y/%m/%d'
    ]
    
    parsed_dates = None
    for date_format in date_formats:
        try:
            if date_format:
                parsed_dates = pd.to_datetime(df['date'], format=date_format, errors='coerce')
            else:
                parsed_dates = pd.to_datetime(df['date'], errors='coerce')
            
            # Break if successful parsing
            if parsed_dates.notna().any():
                break
        except Exception as e:
            print(f"Date parsing error with format {date_format}: {e}")
    
    # If no parsing worked, raise an error
    if parsed_dates is None or parsed_dates.isna().all():
        print("Could not parse date column. Please check your date format.")
        return None
    
    # Replace original date column
    df['date'] = parsed_dates.dt.date
    
    # Convert price to numeric, removing any non-numeric characters
    df['price'] = pd.to_numeric(df['price'].astype(str).str.replace(',', ''), errors='coerce')
    
    # Drop rows with NaN values in required columns
    df = df.dropna(subset=required_columns)
    
    # Select only required columns and reset index
    df_cleaned = df[required_columns].reset_index(drop=True)
    
    return df_cleaned

def insert_data(conn, df):
    """Insert data into the `benchmark_index` table."""
    # Prepare DataFrame
    df = prepare_dataframe(df)
    
    if df is None or df.empty:
        print("No valid data to insert.")
        return
    
    # Create query
    query = """
    INSERT INTO benchmark_index (indexname, date, price)
    VALUES (%s, %s, %s)
    """
    
    # Convert to list of tuples
    records = df.values.tolist()
    
    with conn.cursor() as cur:
        cur.executemany(query, records)
        conn.commit()
    print(f"Inserted {len(records)} records into `benchmark_index`.")

def main():
    print("\n--- CSV IMPORT AND DATABASE LOADER ---")
    print("Select an option:")
    print("1. Debug CSV")
    print("2. Import to Database")
    
    choice = input("Enter your choice (1 or 2): ").strip()

    if choice not in ['1', '2']:
        print("Invalid choice. Please run the script again.")
        return

    # Prompt user for the CSV file location
    csv_file = input("Enter the path to the CSV file: ").strip()
    
    if not os.path.isfile(csv_file):
        print("File not found. Please check the path and try again.")
        return

    if choice == '1':
        # Debugging Mode
        debug_result = debug_csv_import(csv_file)
        if debug_result is not None:
            print("\nCSV Debug Successful!")
            print("Sample of Processed Data:")
            print(debug_result.head())
        else:
            print("\nCSV Debug Failed. Please check your file format.")

    elif choice == '2':
        # Import to Database Mode
        # Read CSV file
        try:
            df = pd.read_csv(csv_file)
            print("CSV Columns:", list(df.columns))
        except Exception as e:
            print(f"Error reading the CSV file: {e}")
            return

        # Connect to the database
        conn = connect_to_db()
        if not conn:
            return

        try:
            # Check if the table exists
            table_exists_flag = table_exists(conn, 'benchmark_index')
            
            if not table_exists_flag:
                create_table(conn, df.columns)
            else:
                # Print existing columns for debugging
                print_table_columns(conn, 'benchmark_index')

            # Prompt for load type
            load_type = input("Choose load type (1: Initial Load, 2: Update): ").strip()
            
            if load_type == '1':
                # Initial Load
                insert_data(conn, df)
            
            elif load_type == '2':
                # Update Data
                if not table_exists_flag:
                    print("Table does not exist. Performing initial load instead.")
                    insert_data(conn, df)
                else:
                    # Get the most recent date from the table
                    most_recent_date = get_most_recent_date(conn)
                    if most_recent_date:
                        # Filter records with date > most_recent_date
                        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
                        new_data = df[df['date'] > most_recent_date]
                        if new_data.empty:
                            print("No new records to insert.")
                        else:
                            insert_data(conn, new_data)
                    else:
                        print("No records found in the table. Performing initial load instead.")
                        insert_data(conn, df)

        except Exception as e:
            print(f"Error processing the data: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
