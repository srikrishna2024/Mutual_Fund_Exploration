import psycopg
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os
import decimal

class MutualFundPerformanceAnalyzer:
    def __init__(self, db_params):
        """
        Initialize database connection parameters
        
        Args:
            db_params (dict): Database connection parameters
        """
        self.db_params = db_params
        # Log file to track the last processed scheme code
        self.LAST_SCHEME_LOG = 'last_processed_performance_scheme.log'
    
    def read_last_processed_scheme(self):
        """
        Read the last processed scheme code from the log file.
        If the file doesn't exist or is empty, return None.
        """
        try:
            if os.path.exists(self.LAST_SCHEME_LOG):
                with open(self.LAST_SCHEME_LOG, 'r') as f:
                    last_scheme = f.read().strip()
                    return last_scheme if last_scheme else None
            return None
        except Exception as e:
            print(f"Error reading log file: {e}")
            return None

    def write_last_processed_scheme(self, scheme_code):
        """
        Write the last processed scheme code to the log file.
        """
        try:
            with open(self.LAST_SCHEME_LOG, 'w') as f:
                f.write(str(scheme_code))
        except Exception as e:
            print(f"Error writing to log file: {e}")

    def create_performance_metrics_table(self, conn):
        """
        Create a new table to store fund performance metrics
        
        Args:
            conn: Database connection
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS mutual_fund_performance_metrics (
            scheme_code VARCHAR(20) PRIMARY KEY,
            scheme_name VARCHAR(255),
            
            -- Monthly Returns
            monthly_mean_return INTEGER,
            monthly_return_volatility INTEGER,
            monthly_positive_return_ratio INTEGER,
            monthly_consistency_score INTEGER,
            
            -- Quarterly Returns
            quarterly_mean_return INTEGER,
            quarterly_return_volatility INTEGER,
            quarterly_positive_return_ratio INTEGER,
            quarterly_consistency_score INTEGER,
            
            -- Yearly Returns
            yearly_mean_return INTEGER,
            yearly_return_volatility INTEGER,
            yearly_positive_return_ratio INTEGER,
            yearly_consistency_score INTEGER,
            
            -- Additional Metadata
            analysis_date DATE,
            total_periods INTEGER
        )
        """
        
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        conn.commit()
        print("Performance metrics table created successfully.")
    
    def safe_round(self, value, default=0):
        """
        Safely round a value, handling NaN and None cases
        
        Args:
            value (float): Value to round
            default (int): Default value if rounding fails
        
        Returns:
            int: Rounded integer value
        """
        try:
            if pd.isna(value) or value is None:
                return default
            return round(value)
        except Exception:
            return default

    def calculate_performance_metrics(self, nav_data, period='monthly'):
        """
        Calculate performance metrics for a given fund
        
        Args:
            nav_data (pd.DataFrame): Net Asset Value data
            period (str): Calculation period - 'monthly', 'quarterly', or 'yearly'
        
        Returns:
            dict: Performance metrics
        """
        # Ensure data is sorted and convert date column to datetime
        nav_data = nav_data.sort_values('date')
        nav_data['date'] = pd.to_datetime(nav_data['date'])
        
        # Convert net_asset_value to float to handle Decimal type
        nav_data['net_asset_value'] = nav_data['net_asset_value'].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
        
        # Set date as index
        nav_data_indexed = nav_data.set_index('date')
        
        # Resample and calculate returns based on period
        if period == 'monthly':
            returns = nav_data_indexed['net_asset_value'].resample('ME').last().pct_change()
        elif period == 'quarterly':
            returns = nav_data_indexed['net_asset_value'].resample('QE').last().pct_change()
        elif period == 'yearly':
            returns = nav_data_indexed['net_asset_value'].resample('YE').last().pct_change()
        else:
            raise ValueError("Invalid period. Choose 'monthly', 'quarterly', or 'yearly'")
        
        # Remove NaN values
        returns = returns.dropna()
        
        # Ensure we have sufficient data
        if len(returns) == 0:
            return {
                f'{period}_mean_return': 0,
                f'{period}_return_volatility': 0,
                f'{period}_positive_return_ratio': 0,
                f'{period}_consistency_score': 0,
                'total_periods': 0
            }
        
        # Calculate metrics and round safely
        metrics = {
            f'{period}_mean_return': self.safe_round(returns.mean() * 100),
            f'{period}_return_volatility': self.safe_round(returns.std() * 100),
            f'{period}_positive_return_ratio': self.safe_round((returns > 0).mean() * 100),
            'total_periods': len(returns)
        }
        
        # Calculate Consistency Score and round safely
        # Higher score means more consistent performance
        consistency_score = (
            (metrics[f'{period}_mean_return']) +
            (-metrics[f'{period}_return_volatility']) +
            (metrics[f'{period}_positive_return_ratio'])
        )
        
        # Normalize consistency score between 0-100 and round safely
        metrics[f'{period}_consistency_score'] = self.safe_round(max(0, min(100, 
            ((consistency_score - (-100)) / (300 - (-100))) * 100
        )))
        
        return metrics
    
    def fetch_nav_data(self, conn, scheme_code=None, start_scheme_code=None, limit=None):
        """
        Fetch NAV data from database
        
        Args:
            conn: Database connection
            scheme_code (str, optional): Specific scheme code to fetch
            start_scheme_code (str, optional): Starting scheme code for batch processing
            limit (int, optional): Limit number of schemes to process
        
        Returns:
            list: List of scheme codes to process
        """
        query = """
        SELECT DISTINCT scheme_code, scheme_name 
        FROM mutual_fund_nav 
        """
        
        conditions = []
        params = []
        
        if scheme_code:
            conditions.append("scheme_code = %s")
            params.append(scheme_code)
        
        if start_scheme_code:
            conditions.append("scheme_code >= %s")
            params.append(start_scheme_code)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY scheme_code"
        
        if limit:
            query += f" LIMIT {limit}"
        
        with conn.cursor() as cur:
            if params:
                cur.execute(query, tuple(params))
            else:
                cur.execute(query)
            return list(cur.fetchall())
    
    def process_fund_performance(self, conn, scheme_code=None, start_scheme_code=None, limit=10000):
        """
        Process performance metrics for specific or multiple funds
        
        Args:
            conn: Database connection
            scheme_code (str, optional): Specific scheme code
            start_scheme_code (str, optional): Starting scheme code for batch processing
            limit (int, optional): Limit number of schemes to process
        """
        # Ensure performance metrics table exists
        self.create_performance_metrics_table(conn)
        
        # Fetch schemes to process
        schemes = self.fetch_nav_data(conn, scheme_code, start_scheme_code, limit)
        
        # Prepare insert query
        insert_query = """
        INSERT INTO mutual_fund_performance_metrics (
            scheme_code, scheme_name,
            monthly_mean_return, monthly_return_volatility, 
            monthly_positive_return_ratio, monthly_consistency_score,
            quarterly_mean_return, quarterly_return_volatility, 
            quarterly_positive_return_ratio, quarterly_consistency_score,
            yearly_mean_return, yearly_return_volatility, 
            yearly_positive_return_ratio, yearly_consistency_score,
            analysis_date, total_periods
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (scheme_code) DO UPDATE SET
            scheme_name = EXCLUDED.scheme_name,
            monthly_mean_return = EXCLUDED.monthly_mean_return,
            monthly_return_volatility = EXCLUDED.monthly_return_volatility,
            monthly_positive_return_ratio = EXCLUDED.monthly_positive_return_ratio,
            monthly_consistency_score = EXCLUDED.monthly_consistency_score,
            quarterly_mean_return = EXCLUDED.quarterly_mean_return,
            quarterly_return_volatility = EXCLUDED.quarterly_return_volatility,
            quarterly_positive_return_ratio = EXCLUDED.quarterly_positive_return_ratio,
            quarterly_consistency_score = EXCLUDED.quarterly_consistency_score,
            yearly_mean_return = EXCLUDED.yearly_mean_return,
            yearly_return_volatility = EXCLUDED.yearly_return_volatility,
            yearly_positive_return_ratio = EXCLUDED.yearly_positive_return_ratio,
            yearly_consistency_score = EXCLUDED.yearly_consistency_score,
            analysis_date = EXCLUDED.analysis_date,
            total_periods = EXCLUDED.total_periods
        """
        
        processed_count = 0
        last_processed_scheme = None
        
        for scheme_code, scheme_name in schemes:
            try:
                # Fetch NAV data for the specific scheme
                nav_query = """
                SELECT date, net_asset_value 
                FROM mutual_fund_nav 
                WHERE scheme_code = %s 
                ORDER BY date
                """
                
                with conn.cursor() as cur:
                    cur.execute(nav_query, (scheme_code,))
                    nav_data = pd.DataFrame(cur.fetchall(), columns=['date', 'net_asset_value'])
                
                # Skip schemes with insufficient data
                if len(nav_data) < 12:  # At least 1 year of data
                    print(f"Skipping {scheme_code} - insufficient data")
                    continue
                
                # Calculate metrics for different periods
                metrics = {}
                for period in ['monthly', 'quarterly', 'yearly']:
                    period_metrics = self.calculate_performance_metrics(nav_data, period)
                    metrics.update(period_metrics)
                
                # Prepare metrics for database insertion
                insert_data = (
                    scheme_code, scheme_name,
                    metrics.get('monthly_mean_return', 0),
                    metrics.get('monthly_return_volatility', 0),
                    metrics.get('monthly_positive_return_ratio', 0),
                    metrics.get('monthly_consistency_score', 0),
                    metrics.get('quarterly_mean_return', 0),
                    metrics.get('quarterly_return_volatility', 0),
                    metrics.get('quarterly_positive_return_ratio', 0),
                    metrics.get('quarterly_consistency_score', 0),
                    metrics.get('yearly_mean_return', 0),
                    metrics.get('yearly_return_volatility', 0),
                    metrics.get('yearly_positive_return_ratio', 0),
                    metrics.get('yearly_consistency_score', 0),
                    datetime.now().date(),
                    metrics.get('total_periods', 0)
                )
                
                # Insert or update metrics
                with conn.cursor() as cur:
                    cur.execute(insert_query, insert_data)
                
                processed_count += 1
                last_processed_scheme = scheme_code
                print(f"Processed {scheme_code} - {scheme_name}")
                
                # Commit every 100 schemes
                if processed_count % 100 == 0:
                    conn.commit()
                    print(f"Committed {processed_count} schemes")
            
            except Exception as e:
                print(f"Error processing {scheme_code}: {e}")
                conn.rollback()
        
        # Final commit
        conn.commit()
        
        # Write the last processed scheme to log file
        if last_processed_scheme:
            self.write_last_processed_scheme(last_processed_scheme)
        
        print(f"Total schemes processed: {processed_count}")
        return processed_count

def main():
    # Database connection parameters
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }
    
    try:
        with psycopg.connect(**DB_PARAMS) as conn:
            analyzer = MutualFundPerformanceAnalyzer(DB_PARAMS)
            
            print("Mutual Fund Performance Analysis")
            print("1. Analyze a specific fund")
            print("2. Analyze batch of 10,000 funds")
            print("3. Analyze all funds")
            
            choice = input("Enter your choice (1, 2, or 3): ")
            
            if choice == '1':
                scheme_code = input("Enter the scheme code: ")
                analyzer.process_fund_performance(conn, scheme_code=scheme_code)
            elif choice == '2':
                # Check if there's a previously processed scheme
                last_processed_scheme = analyzer.read_last_processed_scheme()
                
                if last_processed_scheme:
                    print(f"Continuing from last processed scheme: {last_processed_scheme}")
                    start_scheme_code = last_processed_scheme
                else:
                    start_scheme_code = input("Enter the starting scheme code (optional): ") or None
                
                analyzer.process_fund_performance(conn, start_scheme_code=start_scheme_code)
            elif choice == '3':
                analyzer.process_fund_performance(conn, limit=None)
            else:
                print("Invalid choice.")
    
    except Exception as error:
        print(f"Error: {error}")

if __name__ == "__main__":
    main()
