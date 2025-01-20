import psycopg
import pandas as pd

def upload_csv_to_postgresql(file_path, db_config):
    """
    Uploads data from a CSV file to a PostgreSQL database table.
    This function connects to a PostgreSQL database using the provided configuration,
    checks if the 'mutual_fund_master_data' table exists, creates it if it doesn't,
    and then uploads data from the specified CSV file into the table. The function
    ensures that date columns are properly parsed and handles missing or invalid
    dates by setting them to '9999-12-31'.
    Args:
        file_path (str): The path to the CSV file containing the data to be uploaded.
        db_config (dict): A dictionary containing the database configuration with keys:
            - 'dbname': The name of the database.
            - 'user': The username used to authenticate.
            - 'password': The password used to authenticate.
            - 'host': The host address of the database.
            - 'port': The port number on which the database is listening.
    Raises:
        Exception: If any error occurs during the process, it will be caught and printed.
    """
    try:
        # Connect to the PostgreSQL database
        with psycopg.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        ) as connection:
            with connection.cursor() as cursor:

                # Check if the table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'mutual_fund_master_data'
                    );
                """)
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    # Create the table if it doesn't exist
                    cursor.execute("""
                        CREATE TABLE mutual_fund_master_data (
                            id SERIAL PRIMARY KEY,
                            amc TEXT,
                            code TEXT,
                            scheme_type TEXT,
                            scheme_category TEXT,
                            scheme_name TEXT,
                            launch_date DATE,
                            closure_date DATE
                        );
                    """)
                    connection.commit()
                    print("Table 'mutual_fund_master_data' created.")

                # Load CSV data
                df = pd.read_csv(file_path)

                # Ensure date columns are properly parsed
                date_columns = ['Launch Date', 'Closure Date']
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

                # Replace NaT with None for proper insertion into the database
                df = df.where(pd.notnull(df), None)

                # Update closure_date to 9999-12-31 for blank or invalid entries
                df['Closure Date'] = df['Closure Date'].apply(lambda x: '9999-12-31' if x is None or pd.isna(x) else x)

                # Insert data into the table
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO mutual_fund_master_data (amc, code, scheme_type, scheme_category, scheme_name, launch_date, closure_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, (
                        row['AMC'], row['Code'], row['Scheme Type'],
                        row['Scheme Category'], row['Scheme Name'],
                        row['Launch Date'], row['Closure Date']
                    ))

                connection.commit()
                print("Data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    file_path = input("Enter the path to the CSV file: ")

    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }

    upload_csv_to_postgresql(file_path, DB_PARAMS)
