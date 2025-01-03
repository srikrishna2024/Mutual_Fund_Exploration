import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime
import io
import re

class PortfolioTracker:
    def __init__(self):
        """
        Initialize database connection parameters
        """
        self.DB_PARAMS = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'admin123',
            'host': 'localhost',
            'port': '5432'
        }
    
    def get_db_connection(self):
        """
        Establish and return a database connection
        
        Returns:
            psycopg connection object
        """
        try:
            conn = psycopg.connect(**self.DB_PARAMS)
            return conn
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return None

    def clean_scheme_code(self, scheme_code):
        """
        Clean scheme code by removing commas and extra whitespaces
        
        Args:
            scheme_code (str): Original scheme code
        
        Returns:
            str: Cleaned scheme code
        """
        # Remove commas and extra whitespaces
        return re.sub(r'\s+', '', str(scheme_code)).replace(',', '')

    def ensure_table_exists(self, conn):
        """
        Create portfolio_tracker table if it doesn't exist
        
        Args:
            conn: Database connection
        
        Returns:
            bool: Whether table exists or was successfully created
        """
        try:
            with conn.cursor() as cur:
                # Create table if not exists
                create_table_query = """
                CREATE TABLE IF NOT EXISTS portfolio_tracker (
                    id SERIAL PRIMARY KEY,
                    transaction_date DATE NOT NULL,
                    scheme_code VARCHAR(50) NOT NULL,
                    scheme_name VARCHAR(255) NOT NULL,
                    transaction_type VARCHAR(50) NOT NULL,
                    nav NUMERIC(10, 4) NOT NULL,
                    units NUMERIC(15, 4) NOT NULL,
                    amount NUMERIC(15, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                cur.execute(create_table_query)
                conn.commit()
            return True
        except Exception as e:
            st.error(f"Error creating table: {e}")
            return False

    def validate_transaction_data(self, df):
        """
        Validate the uploaded or manually entered transaction data
        
        Args:
            df (pd.DataFrame): Transaction dataframe
        
        Returns:
            bool: Whether the data is valid
        """
        required_columns = [
            'Transaction Date', 
            'Scheme Code', 
            'Scheme Name', 
            'Transaction Type', 
            'NAV', 
            'Units', 
            'Amount'
        ]
        
        # Check if all required columns are present
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            st.error(f"Missing columns: {', '.join(missing_columns)}")
            return False
        
        # Validate data types and formats
        try:
            df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
            
            # Clean scheme codes
            df['Scheme Code'] = df['Scheme Code'].apply(self.clean_scheme_code)
            
            df['Transaction Type'] = df['Transaction Type'].astype(str)
            df['NAV'] = df['NAV'].astype(float)
            df['Units'] = df['Units'].astype(float)
            df['Amount'] = df['Amount'].astype(float)
        except Exception as e:
            st.error(f"Data type conversion error: {e}")
            return False
        
        return True

    def insert_transactions(self, transactions):
        """
        Insert transactions into portfolio_tracker table
        
        Args:
            transactions (pd.DataFrame): Validated transaction data
        
        Returns:
            bool: Whether insertion was successful
        """
        conn = self.get_db_connection()
        if not conn:
            return False
        
        try:
            # Ensure table exists
            if not self.ensure_table_exists(conn):
                return False
            
            with conn:
                with conn.cursor() as cur:
                    # Prepare insert query
                    insert_query = """
                    INSERT INTO portfolio_tracker (
                        transaction_date, 
                        scheme_code, 
                        scheme_name, 
                        transaction_type, 
                        nav, 
                        units, 
                        amount
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    # Execute batch insert
                    for _, row in transactions.iterrows():
                        cur.execute(insert_query, (
                            row['Transaction Date'],
                            row['Scheme Code'],
                            row['Scheme Name'],
                            row['Transaction Type'],
                            row['NAV'],
                            row['Units'],
                            row['Amount']
                        ))
                
                # Commit the transaction
                conn.commit()
            
            st.success(f"Successfully inserted {len(transactions)} transactions!")
            return True
        except Exception as e:
            st.error(f"Error inserting transactions: {e}")
            return False
        finally:
            if conn:
                conn.close()

def main():
    st.set_page_config(page_title="Mutual Fund Portfolio Tracker", page_icon="💼", layout="wide")
    
    # Initialize portfolio tracker
    tracker = PortfolioTracker()
    
    st.title("🚀 Mutual Fund Portfolio Tracker")
    
    # Sidebar for navigation
    menu = st.sidebar.radio("Select Transaction Input Method", 
                             ["Upload File", "Manual Entry"])
    
    if menu == "Upload File":
        # File upload section
        st.subheader("Upload Transaction Statement")
        uploaded_file = st.file_uploader(
            "Choose a CSV or Excel file", 
            type=['csv', 'xlsx', 'xls']
        )
        
        if uploaded_file is not None:
            try:
                # Read the file based on its type
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                # Display preview of uploaded data
                st.subheader("Transaction Data Preview")
                st.dataframe(df)
                
                # Validate data
                if tracker.validate_transaction_data(df):
                    # Add a confirmation button
                    if st.button("Confirm and Insert Transactions"):
                        tracker.insert_transactions(df)
            
            except Exception as e:
                st.error(f"Error processing file: {e}")
    
    else:
        # Manual entry section
        st.subheader("Manual Transaction Entry")
        
        # Create input fields
        col1, col2 = st.columns(2)
        
        with col1:
            transaction_date = st.date_input("Transaction Date")
            scheme_code = st.text_input("Scheme Code")
            nav = st.number_input("NAV", min_value=0.0, step=0.01)
        
        with col2:
            scheme_name = st.text_input("Scheme Name")
            transaction_type = st.selectbox(
                "Transaction Type", 
                ["Invest", "Redeem", "Switch"]
            )
            units = st.number_input("Units", min_value=0.0, step=0.01)
        
        amount = st.number_input("Amount", min_value=0.0, step=0.1)
        
        # Manual entry submission
        if st.button("Add Transaction"):
            # Create a DataFrame from manual entry
            manual_df = pd.DataFrame([{
                'Transaction Date': transaction_date,
                'Scheme Code': scheme_code,
                'Scheme Name': scheme_name,
                'Transaction Type': transaction_type,
                'NAV': nav,
                'Units': units,
                'Amount': amount
            }])
            
            # Validate and insert
            if tracker.validate_transaction_data(manual_df):
                tracker.insert_transactions(manual_df)

if __name__ == "__main__":
    main()
