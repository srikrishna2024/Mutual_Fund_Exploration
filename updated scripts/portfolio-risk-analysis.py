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

def get_portfolio_funds(df):
    """Get list of funds currently in portfolio"""
    fund_units = df.groupby('code')['units'].sum()
    return fund_units[fund_units > 0].index.tolist()

def get_latest_nav(portfolio_funds):
    """Retrieve the latest NAVs for portfolio funds"""
    with connect_to_db() as conn:
        query = """
            SELECT code, scheme_name, nav as date, value as nav_value
            FROM mutual_fund_nav
            WHERE (code, nav) IN (
                SELECT code, MAX(nav) AS latest_date
                FROM mutual_fund_nav
                WHERE code = ANY(%s)
                GROUP BY code
            )
        """
        return pd.read_sql(query, conn, params=(portfolio_funds,))

def get_historical_nav(portfolio_funds):
    """Retrieve historical NAV data for portfolio funds"""
    with connect_to_db() as conn:
        query = """
            SELECT code, scheme_name, nav as date, value as nav_value
            FROM mutual_fund_nav
            WHERE code = ANY(%s)
            ORDER BY code, nav
        """
        return pd.read_sql(query, conn, params=(portfolio_funds,))

def prepare_cashflows(df):
    """Prepare cashflow data from portfolio transactions"""
    df['cashflow'] = df.apply(lambda x: 
        -x['amount'] if x['transaction_type'] == 'invest'
        else x['amount'] if x['transaction_type'] == 'redeem'
        else (-x['amount'] if x['transaction_type'] == 'switch_out' else x['amount']), 
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
    # Group by scheme and sum units
    df = df.groupby(['scheme_name', 'code']).agg({
        'units': 'sum'
    }).reset_index()
    
    # Filter out funds with zero or negative units
    df = df[df['units'] > 0]

    # Merge with latest NAV data
    df = df.merge(latest_nav[['code', 'nav_value']], on='code', how='left')
    df['current_value'] = df['units'] * df['nav_value']

    total_value = df['current_value'].sum()
    df['weight'] = (df['current_value'] / total_value * 100) if total_value > 0 else 0

    return df

def calculate_xirr(df, latest_nav):
    """Calculate XIRR for portfolio and individual schemes"""
    schemes = df['scheme_name'].unique()
    xirr_results = {}

    portfolio_growth = []  # To store portfolio value for each date

    for scheme in schemes:
        scheme_data = df[df['scheme_name'] == scheme].copy()
        if not scheme_data.empty:
            scheme_nav = latest_nav[latest_nav['code'] == scheme_data['code'].iloc[0]]
            if not scheme_nav.empty:
                latest_value = scheme_data['units'].sum() * scheme_nav['nav_value'].iloc[0]
                final_cf = pd.DataFrame({
                    'date': [datetime.now()],
                    'cashflow': [latest_value]
                })
                scheme_cashflows = scheme_data[['date', 'cashflow']]
                total_cashflows = pd.concat([scheme_cashflows, final_cf])
                rate = xirr(total_cashflows)
                xirr_results[scheme_data['code'].iloc[0]] = round(rate * 100, 1) if rate is not None else 0

    # Calculate portfolio growth and overall XIRR
    unique_dates = sorted(df['date'].unique())
    
    for date in unique_dates:
        transactions_to_date = df[df['date'] <= date].copy()
        transactions_to_date = transactions_to_date.merge(latest_nav[['code', 'nav_value']], on='code', how='left')
        transactions_to_date['current_value'] = transactions_to_date['units'] * transactions_to_date['nav_value']
        total_value = transactions_to_date.groupby('date')['current_value'].sum().loc[date]
        portfolio_growth.append({'date': date, 'value': total_value})

    # Calculate overall portfolio XIRR
    if not df.empty:
        total_value = df.merge(latest_nav[['code', 'nav_value']], on='code', how='left')
        total_value['current_value'] = total_value['units'] * total_value['nav_value']
        final_value = pd.DataFrame({
            'date': [datetime.now()],
            'cashflow': [total_value['current_value'].sum()]
        })
        total_cashflows = pd.concat([df[['date', 'cashflow']], final_value])
        portfolio_xirr = xirr(total_cashflows)
        xirr_results['Portfolio'] = round(portfolio_xirr * 100, 1) if portfolio_xirr is not None else 0

    return xirr_results, pd.DataFrame(portfolio_growth)

def calculate_returns(nav_data, portfolio_funds):
    """Calculate historical returns for portfolio funds"""
    # Filter for portfolio funds
    nav_data = nav_data[nav_data['code'].isin(portfolio_funds)]
    
    # Convert NAV data to pivot table (funds as columns, dates as index)
    nav_pivot = nav_data.pivot(index='date', columns='code', values='nav_value')
    
    # Calculate daily returns
    daily_returns = nav_pivot.pct_change()
    
    # Calculate monthly returns
    monthly_returns = nav_pivot.resample('M').last().pct_change()
    
    return daily_returns, monthly_returns

def calculate_portfolio_metrics(weights_df, returns_df):
    """Calculate portfolio risk metrics"""
    # Get weights as a series
    weights = weights_df.set_index('code')['weight'] / 100
    
    # Filter returns for portfolio funds only
    returns_df = returns_df[weights.index]
    
    # Calculate variance-covariance matrix
    cov_matrix = returns_df.cov()
    
    # Calculate correlation matrix
    corr_matrix = returns_df.corr()
    
    # Calculate portfolio variance
    portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
    
    # Calculate portfolio standard deviation (risk)
    portfolio_std = np.sqrt(portfolio_variance)
    
    # Calculate individual fund volatilities
    fund_volatilities = returns_df.std()
    
    return {
        'covariance_matrix': cov_matrix,
        'correlation_matrix': corr_matrix,
        'portfolio_variance': portfolio_variance,
        'portfolio_std': portfolio_std,
        'fund_volatilities': fund_volatilities
    }

def main():
    st.set_page_config(page_title="Portfolio Risk Analysis", layout="wide")
    st.title("Portfolio Risk Analysis Dashboard")

    try:
        # Get portfolio data
        df = get_portfolio_data()
        
        if df.empty:
            st.warning("No portfolio data found.")
            return

        # Get current portfolio funds
        portfolio_funds = get_portfolio_funds(df)
        
        if not portfolio_funds:
            st.warning("No active funds found in portfolio.")
            return

        # Get NAV data only for portfolio funds
        latest_nav = get_latest_nav(portfolio_funds)
        historical_nav = get_historical_nav(portfolio_funds)

        if latest_nav.empty or historical_nav.empty:
            st.warning("No NAV data found for portfolio funds.")
            return

        # Prepare data
        df['date'] = pd.to_datetime(df['date'])
        df = prepare_cashflows(df)
        historical_nav['date'] = pd.to_datetime(historical_nav['date'])

        # Calculate XIRR and portfolio weights
        xirr_results, portfolio_growth_df = calculate_xirr(df, latest_nav)
        weights_df = calculate_portfolio_weights(df, latest_nav)

        # Calculate historical returns for portfolio funds only
        daily_returns, monthly_returns = calculate_returns(historical_nav, portfolio_funds)

        # Calculate risk metrics
        risk_metrics = calculate_portfolio_metrics(weights_df, monthly_returns)

        # Display sections
        st.header("1. Portfolio Composition")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Current Portfolio Value")
            st.metric("Total Value", f"₹{weights_df['current_value'].sum():,.2f}")
            
        with col2:
            st.subheader("Portfolio XIRR")
            st.metric("XIRR", f"{xirr_results['Portfolio']:.1f}%")

        # Display fund weights with metrics
        st.header("2. Fund-wise Analysis")
        fund_analysis = weights_df.copy()
        fund_analysis['Weight (%)'] = fund_analysis['weight'].round(2)
        fund_analysis['Monthly Volatility (%)'] = fund_analysis['code'].map(
            risk_metrics['fund_volatilities'] * 100
        ).round(2)
        fund_analysis['XIRR (%)'] = fund_analysis['code'].map(xirr_results)
        
        display_columns = [
            'scheme_name', 
            'Weight (%)', 
            'Monthly Volatility (%)', 
            'XIRR (%)', 
            'current_value'
        ]
        
        st.dataframe(fund_analysis[display_columns])

        # Display correlation matrix
        st.header("3. Fund Correlations")
        st.write("Correlation Matrix")
        correlation_display = risk_metrics['correlation_matrix'].copy()
        correlation_display.index = weights_df.set_index('code')['scheme_name']
        correlation_display.columns = weights_df.set_index('code')['scheme_name']
        
        st.dataframe(
            correlation_display.style.format("{:.2f}")
            .background_gradient(cmap='RdYlGn', vmin=-1, vmax=1)
        )

        # Display portfolio risk metrics
        st.header("4. Portfolio Risk Metrics")
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Portfolio Monthly Volatility", 
                f"{risk_metrics['portfolio_std'] * 100:.2f}%"
            )
        
        with col2:
            st.metric(
                "Portfolio Annual Volatility", 
                f"{risk_metrics['portfolio_std'] * np.sqrt(12) * 100:.2f}%"
            )

        # Display historical returns chart
        st.header("5. Historical Performance")
        st.subheader("Monthly Returns by Fund")
        
        # Replace code with scheme_name in monthly returns
        monthly_returns_display = monthly_returns.copy()
        code_to_scheme = weights_df.set_index('code')['scheme_name']
        monthly_returns_display.columns = monthly_returns_display.columns.map(
            lambda x: code_to_scheme.get(x, x)
        )
        st.line_chart(monthly_returns_display)

        # Display portfolio growth
        st.subheader("Portfolio Value Growth")
        st.line_chart(portfolio_growth_df.set_index('date'))

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.error("Please check your database connection and data integrity.")

if __name__ == "__main__":
    main()