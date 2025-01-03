import psycopg
import requests
from datetime import datetime, timedelta
import sys
import os

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

# Log file to track the last processed scheme code
LAST_SCHEME_LOG = 'last_processed_scheme.log'

def read_last_processed_scheme():
    """
    Read the last processed scheme code from the log file.
    If the file doesn't exist or is empty, return None.
    """
    try:
        if os.path.exists(LAST_SCHEME_LOG):
            with open(LAST_SCHEME_LOG, 'r') as f:
                last_scheme = f.read().strip()
                return last_scheme if last_scheme else None
        return None
    except Exception as e:
        print(f"Error reading log file: {e}")
        return None

def write_last_processed_scheme(scheme_code):
    """
    Write the last processed scheme code to the log file.
    """
    try:
        with open(LAST_SCHEME_LOG, 'w') as f:
            f.write(str(scheme_code))
    except Exception as e:
        print(f"Error writing to log file: {e}")

def get_latest_date(conn, scheme_code):
    query = """
    SELECT MAX(date) as latest_date
    FROM mutual_fund_nav
    WHERE scheme_code = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (scheme_code,))
        result = cur.fetchone()
        return result[0] if result else None

def get_latest_dates(conn, start_scheme_code=None, limit=None):
    query = """
    SELECT scheme_code, MAX(date) as latest_date
    FROM mutual_fund_nav
    """
    if start_scheme_code:
        query += " WHERE scheme_code >= %s"
    query += " GROUP BY scheme_code ORDER BY scheme_code"
    if limit:
        query += f" LIMIT {limit}"

    with conn.cursor() as cur:
        if start_scheme_code:
            cur.execute(query, (start_scheme_code,))
        else:
            cur.execute(query)
        return {row[0]: row[1] for row in cur.fetchall()}

def fetch_mf_data(scheme_code, start_date):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        scheme_name = data['meta']['scheme_name']
        nav_data = data['data']

        # Filter data starting from the day after start_date
        filtered_data = [
            (scheme_code, datetime.strptime(entry['date'], '%d-%m-%Y').date(), 
             float(entry['nav']), scheme_name)
            for entry in nav_data
            if datetime.strptime(entry['date'], '%d-%m-%Y').date() > start_date
        ]
        return filtered_data
    else:
        print(f"Failed to fetch data for scheme code {scheme_code}")
        return []

def insert_data(conn, data):
    insert_query = """
    INSERT INTO mutual_fund_nav (scheme_code, date, net_asset_value, scheme_name)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (scheme_code, date) DO UPDATE
    SET net_asset_value = EXCLUDED.net_asset_value
    """

    total_records = len(data)
    records_inserted = 0

    with conn.cursor() as cur:
        for i in range(0, total_records, 5000):
            batch = data[i:i+5000]
            cur.executemany(insert_query, batch)
            conn.commit()
            records_inserted += len(batch)
            print(f"Inserted {records_inserted} out of {total_records} records")

    return records_inserted

def update_multiple_schemes(conn, start_scheme_code, limit=10000):
    latest_dates = get_latest_dates(conn, start_scheme_code, limit)
    print(f"Found {len(latest_dates)} scheme codes in the database starting from {start_scheme_code}.")

    total_new_records = 0
    last_processed_scheme = None

    for scheme_code, latest_date in latest_dates.items():
        print(f"Processing scheme code: {scheme_code}")
        try:
            new_data = fetch_mf_data(scheme_code, latest_date)
            if new_data:
                inserted_records = insert_data(conn, new_data)
                total_new_records += inserted_records
                print(f"Inserted {inserted_records} new records for scheme code {scheme_code}")
            else:
                print(f"No new data for scheme code {scheme_code}")
            
            # Update last processed scheme
            last_processed_scheme = scheme_code
        except Exception as e:
            print(f"Error processing scheme {scheme_code}: {e}")
            break

    # Write the last processed scheme to log file
    if last_processed_scheme:
        write_last_processed_scheme(last_processed_scheme)

    return total_new_records

def main():
    try:
        # Use psycopg.connect instead of psycopg2.connect
        with psycopg.connect(**DB_PARAMS) as conn:
            print("Connected to the database successfully.")

            print("Choose an option:")
            print("1. Update latest data for a specific scheme code")
            print("2. Update latest data for 10000 schemes from a starting or continuing scheme code")
            print("3. Update latest data for all schemes with recent data in current or previous year")
            
            choice = input("Enter your choice (1, 2, or 3): ")

            if choice == '1':
                scheme_code = input("Enter the scheme code: ")
                total_new_records = update_multiple_schemes(conn, scheme_code, limit=1)
            elif choice == '2':
                # Check if there's a previously processed scheme
                last_processed_scheme = read_last_processed_scheme()
                
                if last_processed_scheme:
                    print(f"Continuing from last processed scheme: {last_processed_scheme}")
                    start_scheme_code = last_processed_scheme
                else:
                    start_scheme_code = input("Enter the starting scheme code: ")
                
                total_new_records = update_multiple_schemes(conn, start_scheme_code)
            elif choice == '3':
                total_new_records = update_multiple_schemes(conn, None)
            else:
                print("Invalid choice. Exiting.")
                return

            print(f"Total new records inserted: {total_new_records}")

    except (Exception, psycopg.Error) as error:
        print(f"Error while connecting to PostgreSQL or processing data: {error}", file=sys.stderr)

if __name__ == "__main__":
    main()
