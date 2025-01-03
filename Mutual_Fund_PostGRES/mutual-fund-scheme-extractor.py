import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from tqdm import tqdm
import csv
import os
import importlib.util

def check_dependencies():
    """Check if required dependencies are installed."""
    missing_deps = []
    
    # Check for psycopg
    if importlib.util.find_spec("psycopg") is None:
        missing_deps.append("psycopg")
    
    # Check for requests
    if importlib.util.find_spec("requests") is None:
        missing_deps.append("requests")
        
    # Check for tqdm
    if importlib.util.find_spec("tqdm") is None:
        missing_deps.append("tqdm")
    
    if missing_deps:
        print("Missing required dependencies:", ", ".join(missing_deps))
        print("\nPlease install them using pip:")
        print(f"pip install {' '.join(missing_deps)}")
        return False
    return True

class DatabaseHandler:
    """Handle database operations with proper connection management."""
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
        self.psycopg = None
    
    def connect(self):
        """Establish database connection with error handling."""
        try:
            import psycopg
            from psycopg.rows import dict_row
            self.psycopg = psycopg
            self.conn = psycopg.connect(**self.db_params, row_factory=dict_row)
            return True
        except ImportError as e:
            print("Error: PostgreSQL driver (psycopg) not properly installed.")
            print("Please install it using: pip install psycopg")
            return False
        except Exception as e:
            print(f"Database connection error: {str(e)}")
            return False
    
    def create_table(self):
        """Create the mutual funds table if it doesn't exist."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS mutual_fund_master_data (
                    scheme_code VARCHAR(20) PRIMARY KEY,
                    scheme_name VARCHAR(255),
                    category VARCHAR(100),
                    fund_house VARCHAR(255),
                    scheme_type VARCHAR(100)
                )
                """)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False
    
    def insert_data(self, data):
        """Insert data into the database with error handling."""
        if not self.conn:
            return False
        
        try:
            with self.conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO mutual_fund_master_data 
                        (scheme_code, scheme_name, category, fund_house, scheme_type)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (scheme_code) DO UPDATE
                    SET 
                        scheme_name = EXCLUDED.scheme_name,
                        category = EXCLUDED.category,
                        fund_house = EXCLUDED.fund_house,
                        scheme_type = EXCLUDED.scheme_type
                    """, data)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            return False
    
    def get_category_summary(self):
        """Get summary of mutual fund categories."""
        if not self.conn:
            return []
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT category, COUNT(*) as count 
                    FROM mutual_fund_master_data 
                    GROUP BY category
                """)
                return cur.fetchall()
        except Exception as e:
            print(f"Error fetching category summary: {str(e)}")
            return []
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

def create_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_with_retry(url, max_retries=5):
    session = create_retry_session()
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt == max_retries - 1:
                print(f"Failed to fetch data from {url} after {max_retries} attempts. Error: {str(e)}")
                return None
            time.sleep(2 ** attempt)

def fetch_mutual_fund_list():
    url = "https://api.mfapi.in/mf"
    data = fetch_with_retry(url)
    if data is None:
        raise Exception("Failed to fetch mutual fund list")
    return data

def fetch_scheme_details(scheme_code):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    data = fetch_with_retry(url)
    if data is None:
        print(f"Failed to fetch details for scheme code {scheme_code}")
        return None
    return data['meta']

def save_to_csv(data, filename):
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['scheme_code', 'scheme_name', 'category', 'fund_house', 'scheme_type'])
            writer.writerows(data)
        return True
    except Exception as e:
        print(f"Error saving to CSV: {str(e)}")
        return False

def print_category_summary(fund_details):
    print("\nSummary of categories:")
    category_counts = {}
    for fund in fund_details:
        category = fund[2]
        category_counts[category] = category_counts.get(category, 0) + 1
    for category, count in sorted(category_counts.items()):
        print(f"{category}: {count}")

def main():
    # Check dependencies first
    if not check_dependencies():
        return

    print("Choose an option:")
    print("1. Create a CSV file with the extracted data")
    print("2. Insert data into the PostgreSQL database table")
    choice = input("Enter your choice (1 or 2): ").strip()

    print("Fetching the list of all mutual funds...")
    try:
        mutual_funds = fetch_mutual_fund_list()
    except Exception as e:
        print(f"Error fetching mutual fund list: {str(e)}")
        return

    print(f"Found {len(mutual_funds)} mutual funds. Fetching details for each fund...")
    
    fund_details = []
    for fund in tqdm(mutual_funds, desc="Processing funds"):
        scheme_code = fund['schemeCode']
        details = fetch_scheme_details(scheme_code)
        if details:
            fund_details.append((
                scheme_code,
                fund['schemeName'],
                details.get('scheme_category', 'N/A'),
                details.get('fund_house', 'N/A'),
                details.get('scheme_type', 'N/A')
            ))
        time.sleep(0.1)

    if choice == '1':
        filename = 'mutual_fund_data.csv'
        if save_to_csv(fund_details, filename):
            print(f"Data saved to {filename}")
            print_category_summary(fund_details)
        else:
            print("Failed to save data to CSV file.")

    elif choice == '2':
        DB_PARAMS = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'admin123',
            'host': 'localhost',
            'port': '5432'
        }
        
        db = DatabaseHandler(DB_PARAMS)
        if not db.connect():
            return
        
        print("Connected to the database successfully.")
        
        if not db.create_table():
            db.close()
            return
        print("Table 'mutual_fund_master_data' created or already exists.")
        
        print("Inserting data into the database...")
        if not db.insert_data(fund_details):
            db.close()
            return
        print("Data insertion completed.")
        
        category_counts = db.get_category_summary()
        if category_counts:
            print("\nSummary of categories:")
            for row in category_counts:
                print(f"{row['category']}: {row['count']}")
        
        db.close()
        print("Database connection closed.")
    else:
        print("Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    main()
