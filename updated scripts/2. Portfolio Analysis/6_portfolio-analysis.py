import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg
from scipy.optimize import newton

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
    """Retrieve all records from portfolio_data table"""
    with connect_to_db() as conn:
        query = """
            SELECT date, scheme_name, code, transaction_type, value, units, amount 
            FROM portfolio_data 
            ORDER BY date, scheme_name
        """
        return pd.read_sql(query, conn)

def get_latest_nav():
    """Retrieve the latest NAVs from mutual_fund_nav table"""
    with connect_to_db() as conn:
        query = """
            SELECT code, value AS nav_value
            FROM mutual_fund_nav
            WHERE (code, nav) IN (
                SELECT code, MAX(nav) AS nav_date
                FROM mutual_fund_nav
                GROUP BY code
            )
        """
        return pd.read_sql(query, conn)

def prepare_cashflows(df):
    """Prepare cashflow data from portfolio transactions"""
    df['cashflow'] = df.apply(lambda x: 
        -x['amount'] if x['transaction_type'] == 'invest'
        else x['amount'] if x['transaction_type'] == 'redeem'
        else (-x['amount'] if x['transaction_type'] == 'switch' else 0), 
        axis=1
    )
    return df

def xirr(transactions):
    """Calculate XIRR given a set of transactions"""
    if len(transactions) < 2:
        return None

    def xnpv(rate):
        first_date = pd.to_datetime(transactions['date'].min())
        days = [(pd.to_datetime(date) - first_date).days for date in transactions['date']]
        return sum([cf * (1 + rate) ** (-d/365.0) for cf, d in zip(transactions['cashflow'], days)])

    def xnpv_der(rate):
        first_date = pd.to_datetime(transactions['date'].min())
        days = [(pd.to_datetime(date) - first_date).days for date in transactions['date']]
        return sum([cf * (-d/365.0) * (1 + rate) ** (-d/365.0 - 1) 
                    for cf, d in zip(transactions['cashflow'], days)])

    try:
        return newton(xnpv, x0=0.1, fprime=xnpv_der, maxiter=1000)
    except:
        return None

def calculate_portfolio_weights(df, latest_nav):
    """Calculate current portfolio weights for each scheme"""
    df = df.groupby('scheme_name').agg({
        'units': 'sum',
        'code': 'first'
    }).reset_index()

    df = df.merge(latest_nav, on='code', how='left')
    df['current_value'] = df['units'] * df['nav_value']

    total_value = df['current_value'].sum()
    df['weight'] = (df['current_value'] / total_value) * 100 if total_value > 0 else 0

    return df

def calculate_xirr(df, latest_nav):
    """
    Calculate XIRR (Extended Internal Rate of Return) for a portfolio and individual schemes.

    Parameters:
    df (pd.DataFrame): DataFrame containing transaction details with columns 'scheme_name', 'date', 'units', 'cashflow', and 'code'.
    latest_nav (pd.DataFrame): DataFrame containing the latest NAV values with columns 'code' and 'nav_value'.

    Returns:
    tuple: A tuple containing:
        - xirr_results (dict): A dictionary with scheme names as keys and their respective XIRR values as values. The key 'Portfolio' contains the overall portfolio XIRR.
        - portfolio_growth (pd.DataFrame): A DataFrame with columns 'date' and 'value' representing the portfolio value growth over time.
    """
    """Calculate XIRR for portfolio and individual schemes"""
    schemes = df['scheme_name'].unique()
    xirr_results = {}

    portfolio_growth = []  # To store portfolio value for each date

    for scheme in schemes:
        transactions = df[df['scheme_name'] == scheme].copy()
        # Add the current value as a final cash flow
        if not transactions.empty:
            latest_value = transactions['units'].sum() * latest_nav.loc[latest_nav['code'] == transactions['code'].iloc[0], 'nav_value'].values[0]
            transactions = pd.concat([
                transactions,
                pd.DataFrame({'date': [datetime.now()], 'cashflow': [latest_value]})
            ])
            rate = xirr(transactions)
            xirr_results[scheme] = round(rate * 100, 1) if rate is not None else 0

    # Calculate portfolio growth and overall XIRR
    unique_dates = df['date'].sort_values().unique()

    for date in unique_dates:
        transactions_up_to_date = df[df['date'] <= date].copy()
        transactions_up_to_date = transactions_up_to_date.merge(latest_nav, on='code', how='left')
        transactions_up_to_date['current_value'] = transactions_up_to_date['units'] * transactions_up_to_date['nav_value']
        total_value_on_date = transactions_up_to_date['current_value'].sum()
        portfolio_growth.append({'date': date, 'value': total_value_on_date})

    # Calculate overall portfolio XIRR
    total_transactions = df.copy()
    if not total_transactions.empty:
        total_transactions = total_transactions.merge(latest_nav, on='code', how='left')
        total_transactions['current_value'] = total_transactions['units'] * total_transactions['nav_value']
        portfolio_final_value = pd.DataFrame({
            'date': [datetime.now()],
            'cashflow': [total_transactions['current_value'].sum()]
        })
        total_cashflow = total_transactions[['date', 'cashflow']]
        total_transactions = pd.concat([total_cashflow, portfolio_final_value])
        portfolio_xirr = xirr(total_transactions)
        xirr_results['Portfolio'] = round(portfolio_xirr * 100, 1) if portfolio_xirr is not None else 0

    return xirr_results, pd.DataFrame(portfolio_growth)

def main():
    st.set_page_config(page_title="Portfolio Analysis", layout="wide")
    st.title("Portfolio Analysis Dashboard")

    df = get_portfolio_data()
    latest_nav = get_latest_nav()

    if df.empty or latest_nav.empty:
        st.warning("No data found. Please ensure portfolio data and NAV data are available.")
        return

    df['date'] = pd.to_datetime(df['date'])
    df = prepare_cashflows(df)

    # Calculate XIRR and Portfolio Growth
    xirr_results, portfolio_growth_df = calculate_xirr(df, latest_nav)

    # Calculate portfolio weights
    weights_df = calculate_portfolio_weights(df, latest_nav)

    # Display Overall Portfolio Metrics
    st.subheader("Overall Portfolio Metrics")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Portfolio XIRR", f"{xirr_results['Portfolio']:.1f}%")
    with col2:
        st.metric("Current Portfolio Value", f"{weights_df['current_value'].sum():,.2f}")

    st.metric("Total Invested Amount", f"{df[df['transaction_type'] == 'invest']['amount'].sum():,.2f}")

    # Display Individual Scheme Metrics
    st.subheader("Individual Fund Metrics")
    fund_metrics = weights_df[['scheme_name', 'current_value', 'weight']]
    fund_metrics['XIRR (%)'] = fund_metrics['scheme_name'].map(xirr_results)
    st.dataframe(fund_metrics)

    # Display Portfolio Growth Over Time
    st.subheader("Portfolio Growth Over Time")
    st.line_chart(portfolio_growth_df.rename(columns={'value': 'Portfolio Value'}).set_index('date'))

if __name__ == "__main__":
    main()
