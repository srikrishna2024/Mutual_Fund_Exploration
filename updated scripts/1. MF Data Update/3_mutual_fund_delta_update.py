import psycopg
import requests
from datetime import datetime, timedelta
import os

LOG_FILE = "nav_update_log.txt"

def write_log(message):
    """Writes a message to the log file."""
    with open(LOG_FILE, "a") as file:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file.write(f"[{timestamp}] {message}\n")

def fetch_schemes_to_update(cursor, specific_code=None):
    """
    Fetches schemes from the mutual_fund_nav table where the most recent NAV date 
    is within the last 30 days. If a specific scheme code is provided, 
    only fetch that scheme.
    """
    if specific_code:
        cursor.execute("""
            SELECT DISTINCT code, scheme_name, MAX(nav) AS most_recent_nav_date
            FROM mutual_fund_nav
            WHERE code = %s
            GROUP BY code, scheme_name;
        """, (specific_code,))
    else:
        cursor.execute("""
            SELECT DISTINCT code, scheme_name, MAX(nav) AS most_recent_nav_date
            FROM mutual_fund_nav
            GROUP BY code, scheme_name;
        """)

    schemes = cursor.fetchall()
    valid_schemes = []
    for scheme in schemes:
        most_recent_nav_date = scheme[2]
        if most_recent_nav_date and (datetime.now().date() - most_recent_nav_date).days <= 30:
            valid_schemes.append(scheme)
    return valid_schemes

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

def update_nav_data(cursor, schemes):
    """Updates NAV data for the given list of schemes."""
    total_updated = 0
    for scheme in schemes:
        scheme_code, scheme_name, most_recent_nav_date = scheme
        print(f"Processing scheme: {scheme_code} - {scheme_name}")
        nav_data = fetch_nav_data(scheme_code)
        if nav_data and 'data' in nav_data:
            updated_records = 0
            for nav_entry in nav_data['data']:
                nav_date = datetime.strptime(nav_entry['date'], "%d-%m-%Y").date()
                if nav_date <= most_recent_nav_date:
                    continue  # Skip older NAV data
                nav_value = float(nav_entry['nav'])
                cursor.execute("""
                    INSERT INTO mutual_fund_nav (code, scheme_name, nav, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_code_nav DO NOTHING;
                """, (scheme_code, scheme_name, nav_date, nav_value))
                updated_records += 1
            print(f"Updated {updated_records} records for scheme: {scheme_name}")
            write_log(f"Updated {updated_records} records for scheme: {scheme_name}")
            total_updated += updated_records
        else:
            print(f"No NAV data found for scheme {scheme_code}.")
            write_log(f"No NAV data found for scheme {scheme_code}.")
    print(f"Total NAV records updated: {total_updated}")
    write_log(f"Total NAV records updated: {total_updated}")

def nav_recent_updater(db_config):
    """
    Updates the Net Asset Value (NAV) data for mutual fund schemes in a PostgreSQL database.
    Parameters:
    db_config (dict): A dictionary containing the database configuration with keys:
        - 'dbname': Name of the database
        - 'user': Username for the database
        - 'password': Password for the database
        - 'host': Host address of the database
        - 'port': Port number of the database
    The function provides two options to the user:
    1. Update all schemes
    2. Update a specific scheme by entering its scheme code
    The function fetches the schemes to update based on the user's choice and updates their NAV data.
    It commits the changes to the database and logs the process.
    If an error occurs during the process, it prints the error message and logs it.
    Note:
    - The function assumes the existence of helper functions `fetch_schemes_to_update`, `update_nav_data`, and `write_log`.
    - The function uses the `psycopg` library to connect to the PostgreSQL database.
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
                # Get user's choice
                print("Choose an option:\n1. Update all schemes\n2. Update a specific scheme")
                choice = input("Enter your choice (1/2): ")

                if choice == "1":
                    schemes_to_update = fetch_schemes_to_update(cursor)
                    update_nav_data(cursor, schemes_to_update)
                elif choice == "2":
                    scheme_code = input("Enter the scheme code: ")
                    schemes_to_update = fetch_schemes_to_update(cursor, specific_code=scheme_code)
                    if schemes_to_update:
                        update_nav_data(cursor, schemes_to_update)
                    else:
                        print(f"No eligible schemes found for code {scheme_code}.")
                        write_log(f"No eligible schemes found for code {scheme_code}.")
                else:
                    print("Invalid choice. Exiting.")
                    write_log("Invalid choice made by user.")

                connection.commit()
                print("NAV update completed.")
                write_log("NAV update completed.")

    except Exception as e:
        print(f"An error occurred: {e}")
        write_log(f"An error occurred: {e}")

if __name__ == "__main__":
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }

    nav_recent_updater(DB_PARAMS)
