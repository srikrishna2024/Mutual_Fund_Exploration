import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime
import io
from pathlib import Path
import base64

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

# Initialize database connection
@st.cache_resource
def init_connection():
    try:
        return psycopg.connect(**DB_PARAMS)
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

def create_portfolio_table():
    """Create portfolio_data table if it doesn't exist"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS portfolio_data (
                        id SERIAL PRIMARY KEY,
                        date DATE NOT NULL,
                        scheme_name VARCHAR(255) NOT NULL,
                        code VARCHAR(50) NOT NULL,
                        transaction_type VARCHAR(10) CHECK (transaction_type IN ('invest', 'switch', 'redeem')),
                        value NUMERIC(20, 4),
                        units NUMERIC(20, 4),
                        amount NUMERIC(20, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
        except Exception as e:
            st.error(f"Failed to create table: {str(e)}")

def get_latest_transaction_date():
    """Get the most recent transaction date from the database"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(date) FROM portfolio_data")
                result = cur.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            st.error(f"Failed to get latest transaction date: {str(e)}")
            return None

def clean_numeric_data(df):
    """Clean numeric columns by removing commas and converting to numeric"""
    numeric_columns = ['value', 'units', 'amount']
    for col in numeric_columns:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('₹', '').str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['code'] = df['code'].astype(str).str.replace(',', '')
    return df

def validate_dataframe(df):
    """Validate the uploaded dataframe format and data"""
    required_columns = ['Date', 'scheme_name', 'code', 'Transaction Type', 'value', 'units', 'amount']
    
    if not all(col in df.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in df.columns]
        return False, f"Missing columns: {', '.join(missing_cols)}"
    
    valid_types = {'invest', 'switch', 'redeem'}
    invalid_types = set(df['Transaction Type'].unique()) - valid_types
    if invalid_types:
        return False, f"Invalid transaction types found: {', '.join(invalid_types)}"
    
    try:
        df = clean_numeric_data(df)
        numeric_columns = ['value', 'units', 'amount']
        for col in numeric_columns:
            if df[col].isna().any():
                invalid_rows = df[df[col].isna()].index.tolist()
                return False, f"Invalid numeric values in {col} at rows: {invalid_rows}"
        
        return True, "Validation successful", df
    except Exception as e:
        return False, f"Error processing numeric data: {str(e)}", None

def insert_portfolio_data(df):
    """Insert validated data into portfolio_data table"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                values = [
                    (
                        row['Date'],
                        row['scheme_name'],
                        row['code'],
                        row['Transaction Type'],
                        float(row['value']),
                        float(row['units']),
                        float(row['amount'])
                    )
                    for _, row in df.iterrows()
                ]
                
                cur.executemany("""
                    INSERT INTO portfolio_data 
                    (date, scheme_name, code, transaction_type, value, units, amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, values)
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Failed to insert data: {str(e)}")
            return False

def get_portfolio_data():
    """Retrieve all records from portfolio_data table"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT date, scheme_name, code, transaction_type, value, units, amount 
                FROM portfolio_data 
                ORDER BY date DESC, created_at DESC
            """
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Failed to retrieve data: {str(e)}")
            return pd.DataFrame()

def process_uploaded_file(uploaded_file, filter_date=None):
    """Process uploaded file and return validated dataframe"""
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        if filter_date:
            df = df[df['Date'] > filter_date]
            if df.empty:
                st.warning("No new transactions found after the latest date in the database.")
                return None
        
        is_valid, message, cleaned_df = validate_dataframe(df)
        
        if is_valid:
            st.success(message)
            return cleaned_df
        else:
            st.error(message)
            st.dataframe(df)
            return None
            
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        return None

def initial_data_load():
    st.subheader("Initial Portfolio Data Upload")
    
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=['csv', 'xlsx'],
        key="initial_upload"
    )
    
    if uploaded_file:
        df = process_uploaded_file(uploaded_file)
        if df is not None:
            st.dataframe(df)
            if st.button("Initial Data Load"):
                if insert_portfolio_data(df):
                    st.success(f"Successfully inserted {len(df)} records!")
                    st.rerun()

def add_new_transactions():
    st.subheader("Add New Transactions")
    
    latest_date = get_latest_transaction_date()
    if not latest_date:
        st.warning("No existing transactions found. Please use the Initial Data Load tab first.")
        return
    
    st.info(f"Most recent transaction date: {latest_date}")
    
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=['csv', 'xlsx'],
        key="new_transactions"
    )
    
    if uploaded_file:
        df = process_uploaded_file(uploaded_file, latest_date)
        if df is not None:
            st.dataframe(df)
            if st.button("Insert New Transactions"):
                if insert_portfolio_data(df):
                    st.success(f"Successfully inserted {len(df)} new records!")
                    st.rerun()

def manual_entry():
    st.subheader("Manual Transaction Entry")
    
    with st.form("manual_entry_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            date = st.date_input("Date", datetime.today())
            scheme_name = st.text_input("Scheme Name")
            code = st.text_input("Code")
            transaction_type = st.selectbox(
                "Transaction Type",
                ['invest', 'switch', 'redeem']
            )
        
        with col2:
            value = st.number_input("Value", min_value=0.0, format="%f")
            units = st.number_input("Units", min_value=0.0, format="%f")
            amount = st.number_input("Amount", min_value=0.0, format="%f")
        
        submitted = st.form_submit_button("Insert Transaction")
        
        if submitted:
            if not scheme_name or not code:
                st.error("Scheme Name and Code are required fields.")
                return
            
            df = pd.DataFrame([{
                'Date': date,
                'scheme_name': scheme_name,
                'code': code,
                'Transaction Type': transaction_type,
                'value': value,
                'units': units,
                'amount': amount
            }])
            
            if insert_portfolio_data(df):
                st.success("Transaction inserted successfully!")
                st.rerun()

def export_data():
    st.subheader("Export Transactions")
    
    data = get_portfolio_data()
    if data.empty:
        st.warning("No data available to export.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv = data.to_csv(index=False)
        st.download_button(
            "Download as CSV",
            csv,
            "portfolio_transactions.csv",
            "text/csv",
            key='download-csv'
        )
    
    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            data.to_excel(writer, index=False, sheet_name='Transactions')
        
        st.download_button(
            "Download as Excel",
            buffer.getvalue(),
            "portfolio_transactions.xlsx",
            "application/vnd.ms-excel",
            key='download-excel'
        )

def main():
    st.set_page_config(
        page_title="Portfolio Transaction Manager",
        page_icon="💼",
        layout="wide"
    )
    
    st.title("Portfolio Transaction Manager")
    
    # Create database table if it doesn't exist
    create_portfolio_table()
    
    # Create tabs for different features
    tab1, tab2, tab3, tab4 = st.tabs([
        "Initial Data Load",
        "Add New Transactions",
        "Manual Entry",
        "Export Data"
    ])
    
    with tab1:
        initial_data_load()
    
    with tab2:
        add_new_transactions()
    
    with tab3:
        manual_entry()
    
    with tab4:
        export_data()
    
    # Show existing records
    st.subheader("Current Portfolio Data")
    portfolio_data = get_portfolio_data()
    if not portfolio_data.empty:
        st.dataframe(portfolio_data)
    else:
        st.info("No records found in the database. Please upload a file or add transactions manually.")

if __name__ == "__main__":
    main()