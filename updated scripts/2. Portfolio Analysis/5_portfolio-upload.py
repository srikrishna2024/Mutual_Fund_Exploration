import streamlit as st
import pandas as pd
import psycopg
from io import StringIO
import tempfile

# Database connection parameters from the existing script
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

def connect_to_db():
    """Create database connection"""
    return psycopg.connect(**DB_PARAMS)

def create_portfolio_table():
    """Create portfolio_data table if it doesn't exist"""
    with connect_to_db() as conn:
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

def clean_numeric_data(df):
    """Clean numeric columns by removing commas and converting to numeric"""
    # Remove commas and convert to numeric for value, units, and amount columns
    numeric_columns = ['value', 'units', 'amount']
    for col in numeric_columns:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('₹', '').str.strip()
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove commas from code column
    df['code'] = df['code'].astype(str).str.replace(',', '')
    
    return df

def validate_dataframe(df):
    """Validate the uploaded dataframe format and data"""
    required_columns = ['Date', 'scheme_name', 'code', 'Transaction Type', 'value', 'units', 'amount']
    
    # Check if all required columns exist
    if not all(col in df.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in df.columns]
        return False, f"Missing columns: {', '.join(missing_cols)}"
    
    # Check transaction types
    valid_types = {'invest', 'switch', 'redeem'}
    invalid_types = set(df['Transaction Type'].unique()) - valid_types
    if invalid_types:
        return False, f"Invalid transaction types found: {', '.join(invalid_types)}. Allowed types: invest, switch, redeem"
    
    # Clean and validate numeric data
    try:
        df = clean_numeric_data(df)
        
        # Check for any remaining NaN values in numeric columns
        numeric_columns = ['value', 'units', 'amount']
        for col in numeric_columns:
            if df[col].isna().any():
                invalid_rows = df[df[col].isna()].index.tolist()
                return False, f"Invalid numeric values found in {col} column at rows: {invalid_rows}"
        
        return True, "Validation successful", df
    except Exception as e:
        return False, f"Error processing numeric data: {str(e)}", None

def insert_portfolio_data(df):
    """Insert validated data into portfolio_data table"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            # Prepare the values for insertion
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
            
            # Insert all rows using executemany
            cur.executemany("""
                INSERT INTO portfolio_data 
                (date, scheme_name, code, transaction_type, value, units, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, values)
            
            conn.commit()

def get_portfolio_data():
    """Retrieve all records from portfolio_data table"""
    with connect_to_db() as conn:
        query = """
            SELECT date, scheme_name, code, transaction_type, value, units, amount 
            FROM portfolio_data 
            ORDER BY date DESC, created_at DESC
        """
        return pd.read_sql(query, conn)

def main():
    """
    Main function to handle the Portfolio Transaction Upload page.
    This function sets up the Streamlit page configuration, handles file uploads,
    validates and processes the uploaded data, and displays the current portfolio data.
    Steps:
    1. Set up the Streamlit page configuration and title.
    2. Create the portfolio table if it doesn't exist.
    3. Provide a file uploader for CSV or Excel files.
    4. Read and process the uploaded file.
    5. Validate the data and show a preview.
    6. Insert the validated data into the database upon user confirmation.
    7. Display the current portfolio data from the database.
    Raises:
        Exception: If there is an error processing the uploaded file or inserting data into the database.
    """
    st.set_page_config(page_title='Portfolio Transaction Upload', layout='wide')
    st.title('Portfolio Transaction Upload')
    
    # Create table if it doesn't exist
    create_portfolio_table()
    
    # File upload section
    st.subheader('Upload Transaction Data')
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        try:
            # Read the file
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            # Convert date column to datetime
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            
            # Validate data
            is_valid, message, cleaned_df = validate_dataframe(df)
            
            # Show preview of the data
            st.subheader('Data Preview')
            if is_valid:
                st.success(message)
                st.dataframe(cleaned_df)
                
                # Process button
                if st.button('Insert Data to Database'):
                    try:
                        insert_portfolio_data(cleaned_df)
                        st.success(f'Successfully inserted {len(cleaned_df)} records into the database!')
                    except Exception as e:
                        st.error(f'Error inserting data: {str(e)}')
            else:
                st.error(message)
                st.dataframe(df)
                
        except Exception as e:
            st.error(f'Error processing file: {str(e)}')
    
    # Show existing records
    st.subheader('Current Portfolio Data')
    portfolio_data = get_portfolio_data()
    if not portfolio_data.empty:
        st.dataframe(portfolio_data)
    else:
        st.info('No records found in the database. Please upload a file to add data.')

if __name__ == "__main__":
    main()