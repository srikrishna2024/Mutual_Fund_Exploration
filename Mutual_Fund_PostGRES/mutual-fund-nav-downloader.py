import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import time
from datetime import datetime
import os
import sys
from tqdm import tqdm
import importlib.util

def check_dependencies():
    """Check if required dependencies are installed."""
    missing_deps = []
    
    # Check for required packages
    for package in ["requests", "pandas", "tqdm"]:
        if importlib.util.find_spec(package) is None:
            missing_deps.append(package)
    
    if missing_deps:
        print("Missing required dependencies:", ", ".join(missing_deps))
        print("\nPlease install them using pip:")
        print(f"pip install {' '.join(missing_deps)}")
        return False
    return True

def create_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    """Create a session with retry mechanism."""
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

def ensure_output_directory():
    """Create output directory if it doesn't exist."""
    output_dir = "mutual_fund_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    return output_dir

def download_scheme_codes():
    """Download list of all mutual fund schemes."""
    url = "https://api.mfapi.in/mf"
    session = create_retry_session()
    
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        schemes = response.json()
        df = pd.DataFrame(schemes)
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error downloading scheme codes: {e}")
        sys.exit(1)

def download_mutual_fund_nav(session, scheme_code):
    """Download NAV data for a specific scheme."""
    base_url = "https://api.mfapi.in/mf/"
    
    try:
        # Download the NAV data for the specific scheme
        url = f"{base_url}{scheme_code}"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse the JSON data
        data = response.json()
        
        if 'error' in data:
            raise Exception(f"API Error: {data['error']}")
        
        # Extract the NAV data
        nav_data = data['data']
        
        # Convert to DataFrame
        df = pd.DataFrame(nav_data)
        
        # Convert date string to datetime
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        
        # Rename columns for clarity
        df = df.rename(columns={'nav': 'Net Asset Value'})
        
        # Add scheme information
        df['Scheme Name'] = data['meta']['scheme_name']
        df['Scheme Code'] = scheme_code
        
        return df
    
    except Exception as e:
        raise Exception(f"Failed to download NAV data: {str(e)}")

def download_nav_for_all_schemes(scheme_codes_df, output_dir):
    """Download NAV data for all schemes with progress tracking."""
    session = create_retry_session()
    total_schemes = len(scheme_codes_df)
    successful_downloads = 0
    failed_downloads = []
    
    # Create progress bar
    progress_bar = tqdm(scheme_codes_df.iterrows(), 
                       total=total_schemes,
                       desc="Downloading NAV data",
                       unit="scheme")
    
    for index, row in progress_bar:
        scheme_code = str(row['schemeCode'])
        scheme_name = row['schemeName']
        
        # Update progress bar description
        progress_bar.set_description(f"Processing {scheme_code}")
        
        try:
            # Download NAV data
            nav_data = download_mutual_fund_nav(session, scheme_code)
            
            # Save to CSV
            output_file = os.path.join(output_dir, f"mutual_fund_nav_data_{scheme_code}.csv")
            nav_data.to_csv(output_file, index=False)
            
            successful_downloads += 1
            
        except Exception as e:
            failed_downloads.append((scheme_code, scheme_name, str(e)))
            tqdm.write(f"Error downloading data for {scheme_name} ({scheme_code}): {e}")
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    return successful_downloads, failed_downloads

def save_failed_downloads(failed_downloads, output_dir):
    """Save list of failed downloads to a log file."""
    if failed_downloads:
        log_file = os.path.join(output_dir, "failed_downloads.log")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(log_file, 'w') as f:
            f.write(f"Failed downloads log - {timestamp}\n\n")
            for scheme_code, scheme_name, error in failed_downloads:
                f.write(f"Scheme: {scheme_name}\n")
                f.write(f"Code: {scheme_code}\n")
                f.write(f"Error: {error}\n")
                f.write("-" * 50 + "\n")

def main():
    # Check dependencies
    if not check_dependencies():
        return
    
    try:
        # Create output directory
        output_dir = ensure_output_directory()
        print(f"Files will be saved in: {os.path.abspath(output_dir)}")
        
        # Download scheme codes
        print("\nDownloading list of all mutual fund scheme codes...")
        scheme_codes_df = download_scheme_codes()
        print(f"Found {len(scheme_codes_df)} schemes")
        
        # Save scheme codes list
        scheme_codes_file = os.path.join(output_dir, "scheme_codes.csv")
        scheme_codes_df.to_csv(scheme_codes_file, index=False)
        print(f"Saved scheme codes to: {scheme_codes_file}")
        
        # Download NAV data
        print("\nStarting download of NAV data for all schemes...")
        successful_downloads, failed_downloads = download_nav_for_all_schemes(scheme_codes_df, output_dir)
        
        # Save failed downloads log
        if failed_downloads:
            save_failed_downloads(failed_downloads, output_dir)
        
        # Print summary
        print("\nDownload process completed!")
        print(f"Total schemes: {len(scheme_codes_df)}")
        print(f"Successfully downloaded: {successful_downloads}")
        print(f"Failed downloads: {len(failed_downloads)}")
        
        if failed_downloads:
            print(f"Check {os.path.join(output_dir, 'failed_downloads.log')} for details of failed downloads")
        
    except KeyboardInterrupt:
        print("\nDownload process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
