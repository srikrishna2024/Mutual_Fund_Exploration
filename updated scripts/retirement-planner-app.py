import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime
import numpy as np

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

def format_indian_numbers(number):
    """
    Format numbers in Indian style (lakhs and crores)
    Example: 10000000 -> ₹1,00,00,000
    """
    if number < 0:
        return f"-₹{format_indian_numbers(-number)[1:]}"
    
    s = str(int(number))
    if len(s) > 7:
        # Crores
        l = len(s)
        crore = s[0:l-7]
        left = s[l-7:]
        formatted = f"{','.join([crore[i:i+2] for i in range(0, len(crore), 2)]).lstrip(',')},{left[:2]},{left[2:4]},{left[4:]}"
    else:
        # Lakhs
        l = len(s)
        if l > 5:
            formatted = s[:-5] + "," + s[-5:-3] + "," + s[-3:]
        elif l > 3:
            formatted = s[:-3] + "," + s[-3:]
        else:
            formatted = s
    
    # Add decimal part if exists
    decimal_part = f"{number:.2f}".split('.')[1]
    return f"₹{formatted}.{decimal_part}"

@st.cache_resource
def init_connection():
    try:
        return psycopg.connect(**DB_PARAMS)
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

def create_retirement_table():
    """Create retirement table if it doesn't exist"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS retirement (
                        id SERIAL PRIMARY KEY,
                        analysis_date DATE NOT NULL,
                        current_age INTEGER NOT NULL,
                        retirement_age INTEGER NOT NULL,
                        years_to_retirement INTEGER NOT NULL,
                        current_annual_expenses NUMERIC(20, 2) NOT NULL,
                        current_investments_equity NUMERIC(20, 2),
                        current_investments_debt NUMERIC(20, 2),
                        inflation NUMERIC(5, 2) NOT NULL,
                        equity_increase_yearly NUMERIC(5, 2),
                        debt_increase_yearly NUMERIC(5, 2),
                        equity_returns NUMERIC(5, 2),
                        debt_returns NUMERIC(5, 2),
                        required_retirement_corpus NUMERIC(20, 2),
                        required_monthly_equity NUMERIC(20, 2),
                        required_monthly_debt NUMERIC(20, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
        except Exception as e:
            st.error(f"Failed to create table: {str(e)}")

def insert_retirement_data(data):
    """Insert retirement analysis data into the database"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO retirement (
                        analysis_date, current_age, retirement_age, years_to_retirement,
                        current_annual_expenses, current_investments_equity, current_investments_debt,
                        inflation, equity_increase_yearly, debt_increase_yearly,
                        equity_returns, debt_returns, required_retirement_corpus,
                        required_monthly_equity, required_monthly_debt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data)
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Failed to insert data: {str(e)}")
            return False

def get_retirement_data():
    """Retrieve all retirement analysis data from the database"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT 
                    analysis_date, current_age, retirement_age, years_to_retirement,
                    current_annual_expenses, current_investments_equity, current_investments_debt,
                    inflation, equity_increase_yearly, debt_increase_yearly,
                    equity_returns, debt_returns, required_retirement_corpus,
                    required_monthly_equity, required_monthly_debt,
                    created_at
                FROM retirement 
                ORDER BY created_at DESC
            """
            df = pd.read_sql_query(query, conn)
            
            # Format currency columns
            currency_columns = [
                'current_annual_expenses', 'current_investments_equity', 
                'current_investments_debt', 'required_retirement_corpus',
                'required_monthly_equity', 'required_monthly_debt'
            ]
            
            for col in currency_columns:
                df[col] = df[col].apply(format_indian_numbers)
            
            return df
        except Exception as e:
            st.error(f"Failed to retrieve data: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()

def calculate_future_expenses(current_expenses, inflation, years):
    """Calculate future annual expenses considering inflation"""
    return current_expenses * (1 + inflation/100) ** years

def calculate_required_corpus(future_annual_expenses, expected_return):
    """Calculate required corpus using the 4% rule adjusted for expected returns"""
    withdrawal_rate = max(4, expected_return - 2)  # Conservative withdrawal rate
    return (future_annual_expenses * 100) / withdrawal_rate

def calculate_investment_growth(current_investment, monthly_investment, rate, years, yearly_increase):
    """Calculate yearly investment growth with monthly additions"""
    yearly_values = []
    current_value = current_investment
    
    for year in range(years + 1):  # +1 to include the final year
        yearly_values.append(current_value)
        
        if year < years:  # Don't calculate for the year after retirement
            # Calculate returns on current value
            current_value *= (1 + rate/100)
            
            # Add monthly investments for the year
            monthly_amt = monthly_investment * (1 + yearly_increase/100) ** year
            current_value += monthly_amt * 12
    
    return yearly_values

def calculate_required_monthly_investment(target_amount, current_investment, rate, years, yearly_increase):
    """Calculate required monthly investment to reach target amount"""
    low = 0
    high = target_amount / (12 * years)  # Initial upper bound
    target_value = target_amount
    
    while high - low > 1:
        mid = (low + high) / 2
        future_value = calculate_future_value(current_investment, mid, rate, years, yearly_increase)
        
        if abs(future_value - target_value) < 1000:
            return mid
        elif future_value < target_value:
            low = mid
        else:
            high = mid
    
    return (low + high) / 2

def calculate_future_value(principal, monthly_investment, rate, years, yearly_increase):
    """Calculate future value of investments with monthly additions and yearly increase"""
    future_value = principal * (1 + rate/100) ** years
    
    for year in range(years):
        yearly_investment = monthly_investment * 12 * (1 + yearly_increase/100) ** year
        future_value += yearly_investment * (1 + rate/100) ** (years - year)
    
    return future_value

def format_dataframe_values(df):
    """Format currency values in the dataframe for display"""
    df = df.copy()
    currency_columns = ['Equity', 'Debt', 'Total']
    for col in currency_columns:
        df[col] = df[col].apply(format_indian_numbers)
    return df

def main():
    st.set_page_config(
        page_title="Retirement Planner",
        page_icon="👴",
        layout="wide"
    )
    
    st.title("Retirement Planning Calculator")
    
    create_retirement_table()
    
    with st.form("retirement_planner_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            current_age = st.number_input("Current Age", min_value=18, max_value=100)
            retirement_age = st.number_input("Desired Retirement Age", min_value=current_age + 1, max_value=100)
            years_to_retirement = int(retirement_age - current_age)
            st.info(f"Years to Retirement: {years_to_retirement}")
            current_annual_expenses = st.number_input("Current Annual Expenses", min_value=0.0)
            inflation = st.number_input("Expected Inflation (%)", min_value=0.0, max_value=20.0, value=6.0)
        
        with col2:
            current_investments_equity = st.number_input("Current Investments - Equity", min_value=0.0)
            current_investments_debt = st.number_input("Current Investments - Debt", min_value=0.0)
            equity_returns = st.number_input("Expected Equity Returns (%)", min_value=0.0, max_value=30.0, value=12.0)
            debt_returns = st.number_input("Expected Debt Returns (%)", min_value=0.0, max_value=20.0, value=7.0)
        
        with col3:
            equity_increase_yearly = st.number_input("Yearly Increase in Equity Investments (%)", min_value=0.0, max_value=50.0, value=10.0)
            debt_increase_yearly = st.number_input("Yearly Increase in Debt Investments (%)", min_value=0.0, max_value=50.0, value=5.0)
        
        submitted = st.form_submit_button("Calculate Retirement Plan")
        
        if submitted:
            future_annual_expenses = calculate_future_expenses(
                current_annual_expenses, 
                inflation, 
                years_to_retirement
            )
            
            weighted_return = (equity_returns * 0.6 + debt_returns * 0.4)
            required_corpus = calculate_required_corpus(future_annual_expenses, weighted_return)
            
            equity_portion = required_corpus * 0.6
            debt_portion = required_corpus * 0.4
            
            required_monthly_equity = calculate_required_monthly_investment(
                equity_portion,
                current_investments_equity,
                equity_returns,
                years_to_retirement,
                equity_increase_yearly
            )
            
            required_monthly_debt = calculate_required_monthly_investment(
                debt_portion,
                current_investments_debt,
                debt_returns,
                years_to_retirement,
                debt_increase_yearly
            )
            
            # Calculate year-by-year growth
            equity_growth = calculate_investment_growth(
                current_investments_equity,
                required_monthly_equity,
                equity_returns,
                years_to_retirement,
                equity_increase_yearly
            )
            
            debt_growth = calculate_investment_growth(
                current_investments_debt,
                required_monthly_debt,
                debt_returns,
                years_to_retirement,
                debt_increase_yearly
            )
            
            # Create growth chart data
            years = list(range(current_age, retirement_age + 1))
            chart_data = pd.DataFrame({
                'Age': years,
                'Equity': equity_growth,
                'Debt': debt_growth,
                'Total': [e + d for e, d in zip(equity_growth, debt_growth)]
            })
            
            # Display results
            st.subheader("Retirement Analysis")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Required Retirement Corpus",
                    format_indian_numbers(required_corpus)
                )
                st.metric(
                    "Future Annual Expenses",
                    format_indian_numbers(future_annual_expenses)
                )
            
            with col2:
                st.metric(
                    "Required Monthly Equity Investment",
                    format_indian_numbers(required_monthly_equity)
                )
            
            with col3:
                st.metric(
                    "Required Monthly Debt Investment",
                    format_indian_numbers(required_monthly_debt)
                )
            
            # Create a copy of chart_data for the line chart (without formatting)
            chart_data_raw = chart_data.copy()
            
            # Display investment growth chart
            st.subheader("Investment Growth Projection")
            st.line_chart(
                chart_data_raw.set_index('Age'),
                height=400
            )
            
            # Save retirement data
            retirement_data = (
                datetime.today().date(),
                current_age,
                retirement_age,
                years_to_retirement,
                current_annual_expenses,
                current_investments_equity,
                current_investments_debt,
                inflation,
                equity_increase_yearly,
                debt_increase_yearly,
                equity_returns,
                debt_returns,
                required_corpus,
                required_monthly_equity,
                required_monthly_debt
            )
            
            if insert_retirement_data(retirement_data):
                st.success("Retirement plan saved successfully!")
            
            # Display data table with formatted values
            st.subheader("Year-by-Year Projection")
            st.dataframe(format_dataframe_values(chart_data))
    
    # Display saved retirement plans
    st.subheader("Previous Retirement Plans")
    retirement_data = get_retirement_data()
    if not retirement_data.empty:
        st.dataframe(retirement_data)
    else:
        st.info("No retirement plans saved yet. Please create a new plan using the form above.")

if __name__ == "__main__":
    main()