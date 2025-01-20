import os
import psycopg
import requests
from datetime import datetime

LOG_FILE = "last_downloaded_scheme.log"

# Set date format explicitly for parsing NAV dates
def parse_date(date_str):
    """Parses a date string in DD-MM-YYYY format."""
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        print(f"Invalid date format: {date_str}")
        return None

def create_nav_table_if_not_exists(cursor):
    """Creates the mutual_fund_nav table if it doesn't exist."""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'mutual_fund_nav'
        );
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        cursor.execute("""
            CREATE TABLE mutual_fund_nav (
                id SERIAL PRIMARY KEY,
                code TEXT NOT NULL,
                scheme_name TEXT NOT NULL,
                nav DATE NOT NULL,
                value FLOAT NOT NULL
            );
        """)
        print("Table 'mutual_fund_nav' created.")

    # Ensure the unique constraint exists
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE table_name = 'mutual_fund_nav' AND constraint_name = 'unique_code_nav';
    """)
    constraint_exists = cursor.fetchone()[0]

    if not constraint_exists:
        cursor.execute("""
            ALTER TABLE mutual_fund_nav
            ADD CONSTRAINT unique_code_nav UNIQUE (code, nav);
        """)
        print("Constraint 'unique_code_nav' added.")

def fetch_open_ended_schemes(cursor):
    """Fetches all open-ended schemes."""
    cursor.execute("""
        SELECT code, scheme_name
        FROM mutual_fund_master_data
        WHERE scheme_type = 'Open Ended';
    """)
    schemes = cursor.fetchall()
    print(f"Fetched {len(schemes)} open-ended schemes.")
    return schemes

def fetch_nav_data(scheme_code, retries=3):
    """Fetches NAV data for a specific scheme using MFAPI with retry logic."""
    api_url = f"https://api.mfapi.in/mf/{scheme_code}"
    for attempt in range(retries):
        try:
            response = requests.get(api_url, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to fetch NAV data for scheme {scheme_code}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching NAV data for scheme {scheme_code}: {e}")
            if attempt < retries - 1:
                print("Retrying...")
    return None

def update_nav_data(cursor, schemes, limit=None, offset=0):
    """Updates NAV data for the given list of schemes."""
    schemes_to_fetch = schemes[offset:offset+limit] if limit else schemes
    updated_count = 0
    last_successful_scheme = None

    for scheme in schemes_to_fetch:
        scheme_code, scheme_name = scheme
        print(f"Processing scheme: {scheme_code} - {scheme_name}")
        nav_data = fetch_nav_data(scheme_code)
        if nav_data and 'data' in nav_data:
            for nav_entry in nav_data['data']:
                nav_date = parse_date(nav_entry['date'])
                if not nav_date:
                    continue
                nav_value = float(nav_entry['nav'])
                cursor.execute("""
                    INSERT INTO mutual_fund_nav (code, scheme_name, nav, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_code_nav DO NOTHING;
                """, (scheme_code, scheme_name, nav_date, nav_value))
            updated_count += 1
            last_successful_scheme = scheme_code
            write_last_downloaded_scheme(last_successful_scheme)
        else:
            print(f"No NAV data found for scheme {scheme_code}.")
    print(f"Updated NAV data for {updated_count} schemes.")
    return last_successful_scheme

def read_last_downloaded_scheme():
    """Reads the last downloaded scheme code from the log file."""
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as file:
            return file.read().strip()
    return None

def write_last_downloaded_scheme(scheme_code):
    """Writes the last downloaded scheme code to the log file."""
    with open(LOG_FILE, "w") as file:
        file.write(scheme_code)

def nav_updater(db_config):
    """
    Updates the Net Asset Value (NAV) data for mutual fund schemes in a PostgreSQL database.
    Args:
        db_config (dict): A dictionary containing the database configuration with keys:
            - 'dbname': The name of the database.
            - 'user': The username used to authenticate.
            - 'password': The password used to authenticate.
            - 'host': The host address of the database.
            - 'port': The port number on which the database is listening.
    The function performs the following steps:
        1. Connects to the PostgreSQL database using the provided configuration.
        2. Ensures that the NAV table exists in the database.
        3. Fetches all eligible open-ended mutual fund schemes.
        4. Prompts the user to choose an update option:
            - Update all schemes.
            - Update 5000 schemes starting from the last downloaded scheme.
            - Update a specific scheme based on the scheme code.
        5. Updates the NAV data based on the user's choice.
        6. Commits the transaction to the database.
    Raises:
        Exception: If any error occurs during the database connection or data update process, it prints the error message.
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
                # Ensure NAV table exists
                create_nav_table_if_not_exists(cursor)

                # Fetch eligible schemes
                all_schemes = fetch_open_ended_schemes(cursor)

                # Get user's choice
                print("Choose an option:\n1. Update all schemes\n2. Update 5000 schemes\n3. Update a specific scheme")
                choice = input("Enter your choice (1/2/3): ")

                if choice == "1":
                    update_nav_data(cursor, all_schemes)
                elif choice == "2":
                    # Determine the starting point
                    last_downloaded_scheme = read_last_downloaded_scheme()
                    if last_downloaded_scheme:
                        offset = next((i for i, scheme in enumerate(all_schemes) if scheme[0] == last_downloaded_scheme), 0) + 1
                    else:
                        offset = 0
                    
                    limit = 5000
                    last_scheme = update_nav_data(cursor, all_schemes, limit=limit, offset=offset)
                    if last_scheme:
                        write_last_downloaded_scheme(last_scheme)
                elif choice == "3":
                    scheme_code = input("Enter the scheme code: ")
                    specific_scheme = [scheme for scheme in all_schemes if scheme[0] == scheme_code]
                    if specific_scheme:
                        update_nav_data(cursor, specific_scheme)
                    else:
                        print(f"No scheme found with code {scheme_code}.")
                else:
                    print("Invalid choice. Exiting.")

                connection.commit()
                print("NAV update completed.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }

    nav_updater(DB_PARAMS)
