import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
import psycopg
import traceback
import numpy as np
from scipy.optimize import newton

def connect_to_db():
    """Create database connection with error handling"""
    try:
        DB_PARAMS = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'admin123',
            'host': 'localhost',
            'port': '5432'
        }
        conn = psycopg.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

def get_available_funds():
    """Get list of unique funds from portfolio_data table"""
    conn = connect_to_db()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT DISTINCT pd.scheme_name, pd.code
            FROM portfolio_data pd
            WHERE EXISTS (
                SELECT 1 
                FROM mutual_fund_nav nav 
                WHERE nav.code = pd.code
            )
            ORDER BY pd.scheme_name
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error fetching funds: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_fund_nav_data(fund_codes):
    """Get historical NAV data for selected funds"""
    conn = connect_to_db()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT code, scheme_name, nav as date, value as nav_value
            FROM mutual_fund_nav
            WHERE code = ANY(%s)
            AND value > 0
            ORDER BY code, nav
        """
        return pd.read_sql(query, conn, params=(fund_codes,))
    except Exception as e:
        st.error(f"Error fetching NAV data: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_fund_transactions(fund_codes):
    """Get transaction data for selected funds"""
    conn = connect_to_db()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT date, scheme_name, code, transaction_type, units, amount
            FROM portfolio_data
            WHERE code = ANY(%s)
            AND units > 0
            ORDER BY date
        """
        return pd.read_sql(query, conn, params=(fund_codes,))
    except Exception as e:
        st.error(f"Error fetching transaction data: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

def xirr(transactions):
    """Calculate XIRR given a set of cash flows"""
    if not transactions or len(transactions) < 2:
        return None
    
    def xnpv(rate, cashflows):
        """Calculate XNPV given a rate and cashflows"""
        t0 = min(cf['date'] for cf in cashflows)
        return sum(
            cf['amount'] / (1 + rate) ** ((cf['date'] - t0).days / 365)
            for cf in cashflows
        )
    
    def xirr_objective(rate, cashflows):
        return xnpv(rate, cashflows)
    
    try:
        return newton(
            lambda r: xirr_objective(r, transactions),
            x0=0.1,  # Initial guess of 10%
            tol=0.0001,
            maxiter=1000
        )
    except:
        return None

def calculate_fund_xirr(transactions_df, nav_df, fund_code=None):
    """
    Calculate XIRR for a specific fund or total portfolio
    If fund_code is None, calculates for entire portfolio
    """
    try:
        # Prepare cash flows
        cashflows = []
        
        # Filter transactions for specific fund if provided
        relevant_transactions = (
            transactions_df[transactions_df['code'] == fund_code]
            if fund_code is not None
            else transactions_df
        )
        
        # Add all investments/redemptions
        for _, row in relevant_transactions.iterrows():
            amount = row['amount']
            if row['transaction_type'] not in ('invest', 'switch_in'):
                amount = -amount
            
            cashflows.append({
                'date': row['date'],
                'amount': -amount  # Negative because investments are outflows
            })
        
        # Add current value as final cash flow
        latest_date = nav_df['date'].max()
        
        if fund_code is not None:
            # Calculate for specific fund
            latest_nav = nav_df[
                (nav_df['date'] == latest_date) & 
                (nav_df['code'] == fund_code)
            ]['nav_value'].iloc[0]
            
            current_units = sum(
                row['units'] if row['transaction_type'] in ('invest', 'switch_in')
                else -row['units']
                for _, row in relevant_transactions.iterrows()
            )
            
            current_value = current_units * latest_nav
        else:
            # Calculate for entire portfolio
            current_value = 0
            for code in transactions_df['code'].unique():
                fund_transactions = transactions_df[transactions_df['code'] == code]
                fund_nav = nav_df[
                    (nav_df['date'] == latest_date) & 
                    (nav_df['code'] == code)
                ]['nav_value'].iloc[0]
                
                fund_units = sum(
                    row['units'] if row['transaction_type'] in ('invest', 'switch_in')
                    else -row['units']
                    for _, row in fund_transactions.iterrows()
                )
                
                current_value += fund_units * fund_nav
        
        if current_value > 0:
            cashflows.append({
                'date': latest_date,
                'amount': current_value
            })
        
        return xirr(cashflows)
    except Exception as e:
        st.error(f"Error calculating XIRR: {str(e)}")
        return None

def calculate_portfolio_value(transactions_df, nav_df):
    """Calculate daily portfolio value for each fund"""
    try:
        # Convert dates to datetime
        transactions_df['date'] = pd.to_datetime(transactions_df['date'])
        nav_df['date'] = pd.to_datetime(nav_df['date'])
        
        # Get unique dates from NAV data
        all_dates = sorted(nav_df['date'].unique())
        portfolio_values = []
        
        # Calculate value for each fund on each date
        for date in all_dates:
            # Get transactions up to this date
            transactions_to_date = transactions_df[transactions_df['date'] <= date].copy()
            
            daily_values = {'date': date}
            
            # Calculate for each fund
            for code in transactions_to_date['code'].unique():
                fund_transactions = transactions_to_date[transactions_to_date['code'] == code]
                net_units = sum(
                    row['units'] if row['transaction_type'] in ('invest', 'switch_in')
                    else -row['units']
                    for _, row in fund_transactions.iterrows()
                )
                
                # Get NAV for this date and fund
                current_nav = nav_df[
                    (nav_df['date'] == date) & 
                    (nav_df['code'] == code)
                ]['nav_value'].iloc[0] if not nav_df[
                    (nav_df['date'] == date) & 
                    (nav_df['code'] == code)
                ].empty else 0
                
                fund_value = net_units * current_nav
                if fund_value > 0:  # Only add non-zero values
                    scheme_name = transactions_df[
                        transactions_df['code'] == code
                    ]['scheme_name'].iloc[0]
                    daily_values[scheme_name] = fund_value
            
            if daily_values:  # Add the date's values if we have any fund values
                portfolio_values.append(daily_values)
        
        return pd.DataFrame(portfolio_values)
    except Exception as e:
        st.error(f"Error calculating portfolio value: {str(e)}")
        st.error(traceback.format_exc())
        return pd.DataFrame()

def format_value(x, p):
    """Format currency values in Indian format (with lakhs and crores)"""
    if x >= 10000000:  # crores
        return f'₹{x/10000000:.1f}Cr'
    elif x >= 100000:  # lakhs
        return f'₹{x/100000:.1f}L'
    elif x >= 1000:  # thousands
        return f'₹{x/1000:.1f}K'
    else:
        return f'₹{x:.0f}'
def calculate_time_based_returns(portfolio_value_df, months):
    """Calculate absolute value change over specified number of months"""
    try:
        if len(portfolio_value_df) < 2:
            return None
            
        end_date = portfolio_value_df['date'].max()
        start_date = end_date - pd.Timedelta(days=30*months)
        
        # Get values at start and end dates
        end_values = portfolio_value_df[portfolio_value_df['date'] == end_date].iloc[0]
        start_df = portfolio_value_df[portfolio_value_df['date'] <= start_date]
        
        if start_df.empty:
            return None
            
        start_values = start_df.iloc[-1]  # Get the closest date before start_date
        
        # Calculate absolute changes for all columns except 'date'
        changes = {}
        for column in portfolio_value_df.columns:
            if column != 'date':
                start_val = start_values[column]
                end_val = end_values[column]
                changes[column] = end_val - start_val
                    
        return changes
    except Exception as e:
        st.error(f"Error calculating {months}-month returns: {str(e)}")
        return None
def main():
    """
    Main function to run the Portfolio Value Tracker Streamlit application.
    This function sets up the Streamlit page configuration, retrieves available funds,
    allows the user to select funds to track, fetches NAV and transaction data for the
    selected funds, calculates portfolio values, and displays various metrics and plots.
    The function performs the following steps:
    1. Sets the page title and layout.
    2. Retrieves available funds from the database.
    3. Allows the user to select funds to track using a multi-select widget.
    4. Fetches NAV and transaction data for the selected funds.
    5. Calculates portfolio values over time.
    6. Plots the portfolio value progression over time.
    7. Calculates and displays time-based returns (1-month, 3-month, 6-month).
    8. Displays metrics for each selected fund, including current value and XIRR.
    9. Displays total portfolio metrics, including total value, portfolio XIRR, and gains/losses.
    10. Provides an expandable section to view raw data.
    If any errors occur during the execution, they are caught and displayed as error messages.
    Raises:
        Exception: If any error occurs during the execution of the function.
    """
    st.set_page_config(page_title="Portfolio Value Tracker", layout="wide")
    st.title("Portfolio Value Tracker")
    
    try:
        # Get available funds
        available_funds = get_available_funds()
        
        if available_funds.empty:
            st.error("No funds found in the database. Please check your data.")
            return
        
        # Multi-select for funds
        selected_funds = st.multiselect(
            "Select Funds to Track",
            options=available_funds['scheme_name'].tolist(),
            default=available_funds['scheme_name'].tolist()[:3]  # Default to first 3 funds
        )
        
        if not selected_funds:
            st.warning("Please select at least one fund to view the portfolio value.")
            return
        
        # Get selected fund codes
        selected_codes = available_funds[
            available_funds['scheme_name'].isin(selected_funds)
        ]['code'].tolist()
        
        with st.spinner("Fetching data..."):
            # Get data for selected funds
            nav_data = get_fund_nav_data(selected_codes)
            transaction_data = get_fund_transactions(selected_codes)
            
            if nav_data.empty:
                st.error("No NAV data available for selected funds.")
                return
            
            if transaction_data.empty:
                st.error("No transaction data available for selected funds.")
                return
            
            # Calculate portfolio value
            portfolio_value_df = calculate_portfolio_value(transaction_data, nav_data)
            
            if portfolio_value_df.empty:
                st.error("Could not calculate portfolio values. Please check the data.")
                return
            
            # Create plot
            st.subheader("Portfolio Value Over Time")
            
            fig, ax = plt.subplots(figsize=(12, 6))
            sns.set_style("whitegrid")
            
            # Plot line for each fund
            value_columns = [col for col in portfolio_value_df.columns if col != 'date']
            for column in value_columns:
                sns.lineplot(
                    data=portfolio_value_df,
                    x='date',
                    y=column,
                    label=column,
                    ax=ax
                )
            
            # Set y-axis to log scale
            ax.set_yscale('log')
            
            # Customize plot
            ax.set_title("Portfolio Value Progression", pad=20)
            ax.set_xlabel("Date")
            ax.set_ylabel("Portfolio Value (₹) - Log Scale")
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            
            # Format y-axis to show values in thousands/lakhs
            ax.yaxis.set_major_formatter(plt.FuncFormatter(format_value))
            
            # Rotate x-axis labels
            plt.xticks(rotation=45)
            
            # Adjust layout to prevent label cutoff
            plt.tight_layout()
            
            # Display plot
            st.pyplot(fig)
            
            # Calculate time-based returns
            one_month_returns = calculate_time_based_returns(portfolio_value_df, 1)
            three_month_returns = calculate_time_based_returns(portfolio_value_df, 3)
            six_month_returns = calculate_time_based_returns(portfolio_value_df, 6)
            
        # Display metrics for each fund
        st.subheader("Returns Summary")
        
        # Calculate number of funds
        num_funds = len(selected_codes)
        
        # Create columns for each fund
        cols = st.columns(num_funds)
        
        # Display metrics for each fund
        for idx, code in enumerate(selected_codes):
            fund_name = available_funds[
                available_funds['code'] == code
            ]['scheme_name'].iloc[0]
            
            with cols[idx]:
                # Container for fund metrics
                st.markdown(f"### {fund_name}")
                
                # Current Value
                latest_value = portfolio_value_df[fund_name].iloc[-1]
                st.metric(
                    "Current Value",
                    format_value(latest_value, None)
                )
                
                # XIRR
                fund_xirr = calculate_fund_xirr(transaction_data, nav_data, code)
                st.metric(
                    "XIRR",
                    f"{fund_xirr * 100:.2f}%" if fund_xirr is not None else "N/A"
                )
        
        # Display total portfolio metrics
        st.subheader("Total Portfolio Summary")
        total_cols = st.columns(5)  # Changed to 5 columns to include 1-month gains
        
        with total_cols[0]:
            total_value = sum(portfolio_value_df[fund].iloc[-1] for fund in selected_funds)
            st.metric(
                "Total Portfolio Value",
                format_value(total_value, None)
            )
        
        with total_cols[1]:
            total_xirr = calculate_fund_xirr(transaction_data, nav_data)
            st.metric(
                "Portfolio XIRR",
                f"{total_xirr * 100:.2f}%" if total_xirr is not None else "N/A",
                help="XIRR considers the timing and size of all investments and current value"
            )
        
        with total_cols[2]:
            portfolio_1m = sum(v for v in one_month_returns.values()) if one_month_returns else None
            st.metric(
                "1-Month Gain/Loss",
                format_value(portfolio_1m, None) if portfolio_1m is not None else "N/A"
            )
        
        with total_cols[3]:
            portfolio_3m = sum(v for v in three_month_returns.values()) if three_month_returns else None
            st.metric(
                "3-Month Gain/Loss",
                format_value(portfolio_3m, None) if portfolio_3m is not None else "N/A"
            )
        
        with total_cols[4]:
            portfolio_6m = sum(v for v in six_month_returns.values()) if six_month_returns else None
            st.metric(
                "6-Month Gain/Loss",
                format_value(portfolio_6m, None) if portfolio_6m is not None else "N/A"
            )
        
        # Display raw data in expandable section
        with st.expander("View Raw Data"):
            st.dataframe(
                portfolio_value_df.set_index('date').sort_index(ascending=False)
            )
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.error(traceback.format_exc())

if __name__ == "__main__":
    main()