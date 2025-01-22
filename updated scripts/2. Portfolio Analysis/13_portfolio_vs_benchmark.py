import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
import psycopg

def connect_to_db():
    """Create database connection"""
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }
    return psycopg.connect(**DB_PARAMS)

def get_portfolio_data():
    """Retrieve portfolio data with scheme details"""
    with connect_to_db() as conn:
        query = """
            SELECT p.date, p.scheme_name, p.code, p.transaction_type, 
                   p.value, p.units, p.amount,
                   n.value as nav_value
            FROM portfolio_data p
            LEFT JOIN mutual_fund_nav n ON p.code = n.code
            AND n.nav = (
                SELECT MAX(nav)
                FROM mutual_fund_nav
                WHERE code = p.code
            )
            ORDER BY p.date
        """
        return pd.read_sql(query, conn)

def get_benchmark_data():
    """Retrieve benchmark data"""
    with connect_to_db() as conn:
        query = """
            SELECT date, price
            FROM benchmark
            ORDER BY date
        """
        return pd.read_sql(query, conn)

def calculate_growth(portfolio_df, benchmark_df, selected_fund):
    """Calculate growth for both portfolio and benchmark investments"""
    # Filter for selected fund
    fund_data = portfolio_df[portfolio_df['scheme_name'] == selected_fund].copy()
    
    # Calculate cumulative investment and units for the fund
    fund_data['cumulative_investment'] = fund_data[fund_data['transaction_type'] == 'invest']['amount'].cumsum()
    fund_data['cumulative_units'] = fund_data['units'].cumsum()
    
    # Calculate daily fund value using latest NAV
    fund_data['current_value'] = fund_data['cumulative_units'] * fund_data['nav_value']
    
    # For benchmark calculation
    benchmark_df = benchmark_df.copy()
    benchmark_start_value = benchmark_df.iloc[0]['price']
    
    # Calculate benchmark investment growth
    # For each investment in the fund, calculate equivalent benchmark units
    benchmark_units = []
    total_benchmark_units = 0
    
    for _, row in fund_data[fund_data['transaction_type'] == 'invest'].iterrows():
        benchmark_value_on_date = benchmark_df[benchmark_df['date'] <= row['date']]['price'].iloc[-1]
        benchmark_units_bought = row['amount'] / benchmark_value_on_date
        total_benchmark_units += benchmark_units_bought
        benchmark_units.append(total_benchmark_units)
    
    # Create benchmark growth series
    benchmark_growth = []
    for date in fund_data['date'].unique():
        if date in benchmark_df['date'].values:
            benchmark_value = benchmark_df[benchmark_df['date'] <= date]['price'].iloc[-1]
            current_units = fund_data[fund_data['date'] <= date]
            current_units = current_units[current_units['transaction_type'] == 'invest']
            if not current_units.empty:
                total_units = current_units['units'].sum()
                benchmark_growth.append({
                    'date': date,
                    'value': benchmark_value * total_benchmark_units
                })
    
    benchmark_growth_df = pd.DataFrame(benchmark_growth)
    
    return fund_data[['date', 'current_value']], benchmark_growth_df

def main():
    st.set_page_config(page_title="Fund vs Benchmark Comparison", layout="wide")
    st.title("Fund vs Benchmark Comparison")
    
    # Load data
    portfolio_df = get_portfolio_data()
    benchmark_df = get_benchmark_data()
    
    if portfolio_df.empty or benchmark_df.empty:
        st.warning("No data found. Please ensure portfolio and benchmark data are available.")
        return
    
    # Convert dates
    portfolio_df['date'] = pd.to_datetime(portfolio_df['date'])
    benchmark_df['date'] = pd.to_datetime(benchmark_df['date'])
    
    # Create fund selector
    available_funds = portfolio_df['scheme_name'].unique()
    selected_fund = st.selectbox("Select Fund", available_funds)
    
    # Calculate growth
    fund_growth, benchmark_growth = calculate_growth(portfolio_df, benchmark_df, selected_fund)
    
    # Create plot
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(data=fund_growth, x='date', y='current_value', label=selected_fund, ax=ax)
    sns.lineplot(data=benchmark_growth, x='date', y='value', label='Nifty50 TRI', ax=ax)
    
    plt.title(f"{selected_fund} vs Nifty50 TRI")
    plt.xlabel("Date")
    plt.ylabel("Value (₹)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    
    # Display plot
    st.pyplot(fig)
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        initial_investment = portfolio_df[
            (portfolio_df['scheme_name'] == selected_fund) & 
            (portfolio_df['transaction_type'] == 'invest')
        ]['amount'].sum()
        st.metric("Total Investment", f"₹{initial_investment:,.2f}")
    
    with col2:
        current_fund_value = fund_growth['current_value'].iloc[-1]
        st.metric("Current Fund Value", f"₹{current_fund_value:,.2f}")
    
    with col3:
        current_benchmark_value = benchmark_growth['value'].iloc[-1]
        st.metric("Nifty50 TRI Value", f"₹{current_benchmark_value:,.2f}")
    
    # Calculate and display returns
    fund_returns = ((current_fund_value - initial_investment) / initial_investment) * 100
    benchmark_returns = ((current_benchmark_value - initial_investment) / initial_investment) * 100
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Fund Returns", f"{fund_returns:.2f}%")
    with col2:
        st.metric("Nifty50 TRI Returns", f"{benchmark_returns:.2f}%")

if __name__ == "__main__":
    main()