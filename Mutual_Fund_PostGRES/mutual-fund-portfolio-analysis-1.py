import streamlit as st
import pandas as pd
import psycopg
import plotly.graph_objs as go
from datetime import datetime

class PortfolioAnalyzer:
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

    def fetch_portfolio_transactions(self):
        """
        Fetch all transactions from portfolio_tracker
        
        Returns:
            pandas DataFrame of portfolio transactions
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            query = """
            SELECT 
                scheme_code, 
                scheme_name, 
                transaction_date, 
                transaction_type, 
                nav, 
                units, 
                amount
            FROM portfolio_tracker
            ORDER BY scheme_code, transaction_date
            """
            
            df = pd.read_sql(query, conn)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            return df
        except Exception as e:
            st.error(f"Error fetching portfolio transactions: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_latest_nav_for_funds(self, scheme_codes):
        """
        Fetch the latest NAV for given scheme codes
        
        Args:
            scheme_codes (list): List of unique scheme codes
        
        Returns:
            Dictionary of latest NAV values
        """
        conn = self.get_db_connection()
        if not conn:
            return {}
        
        try:
            # Create a comma-separated string of scheme codes for the query
            scheme_code_str = ', '.join([f"'{code}'" for code in scheme_codes])
            
            query = f"""
            WITH RankedNavs AS (
                SELECT 
                    scheme_code, 
                    net_asset_value,
                    date,
                    ROW_NUMBER() OVER (
                        PARTITION BY scheme_code 
                        ORDER BY date DESC
                    ) as rank
                FROM mutual_fund_nav
                WHERE scheme_code IN ({scheme_code_str})
            )
            SELECT 
                scheme_code, 
                net_asset_value as latest_nav,
                date as latest_nav_date
            FROM RankedNavs
            WHERE rank = 1
            """
            
            latest_navs = pd.read_sql(query, conn)
            
            # Convert to dictionary for easy lookup
            return dict(zip(latest_navs['scheme_code'], 
                            zip(latest_navs['latest_nav'], 
                                latest_navs['latest_nav_date'])))
        except Exception as e:
            st.error(f"Error fetching latest NAV: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def analyze_portfolio(self):
        """
        Comprehensive portfolio analysis
        
        Returns:
            DataFrame with portfolio analysis results
        """
        # Fetch portfolio transactions
        transactions = self.fetch_portfolio_transactions()
        if transactions is None:
            return None
        
        # Group transactions by scheme
        grouped = transactions.groupby('scheme_code')
        
        # Prepare results storage
        portfolio_analysis = []
        
        # Get unique scheme codes
        scheme_codes = transactions['scheme_code'].unique().tolist()
        
        # Fetch latest NAVs
        latest_navs = self.get_latest_nav_for_funds(scheme_codes)
        
        # Analyze each scheme
        for scheme_code, scheme_group in grouped:
            # Calculate cumulative units and total invested amount
            cumulative_transactions = []
            current_units = 0
            total_invested = 0
            
            # Sort transactions by date
            scheme_group_sorted = scheme_group.sort_values('transaction_date')
            
            for _, transaction in scheme_group_sorted.iterrows():
                if transaction['transaction_type'] == 'Invest':
                    current_units += transaction['units']
                    total_invested += transaction['amount']
                elif transaction['transaction_type'] == 'Redeem':
                    current_units -= transaction['units']
                
                cumulative_transactions.append({
                    'date': transaction['transaction_date'],
                    'cumulative_units': current_units,
                    'total_invested': total_invested
                })
            
            # Get latest NAV for this scheme
            latest_nav_info = latest_navs.get(scheme_code, (None, None))
            latest_nav, latest_nav_date = latest_nav_info
            
            # Prepare analysis result
            portfolio_analysis.append({
                'scheme_code': scheme_code,
                'scheme_name': scheme_group.iloc[0]['scheme_name'],
                'current_units': current_units,
                'total_invested': total_invested,
                'latest_nav': latest_nav,
                'latest_nav_date': latest_nav_date,
                'current_value': current_units * (latest_nav or 0),
                'transactions': cumulative_transactions
            })
        
        return portfolio_analysis

def main():
    st.set_page_config(page_title="Mutual Fund Portfolio Analysis", page_icon="📊", layout="wide")
    
    # Initialize portfolio analyzer
    analyzer = PortfolioAnalyzer()
    
    st.title("🚀 Mutual Fund Portfolio Analysis")
    
    # Perform portfolio analysis
    portfolio_analysis = analyzer.analyze_portfolio()
    
    if portfolio_analysis is None:
        st.error("Could not retrieve portfolio data.")
        return
    
    # Portfolio Summary
    st.subheader("Portfolio Summary")
    
    # Create columns for summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate total portfolio metrics
    total_invested = sum(fund['total_invested'] for fund in portfolio_analysis)
    total_current_value = sum(fund['current_value'] for fund in portfolio_analysis)
    total_gain_loss = total_current_value - total_invested
    total_return_percentage = (total_gain_loss / total_invested * 100) if total_invested else 0
    
    with col1:
        st.metric("Total Invested", f"₹{total_invested:,.2f}")
    with col2:
        st.metric("Current Portfolio Value", f"₹{total_current_value:,.2f}")
    with col3:
        st.metric("Total Gain/Loss", 
                  f"₹{total_gain_loss:,.2f}", 
                  f"{total_return_percentage:.2f}%",
                  delta_color="normal")
    with col4:
        st.metric("Number of Funds", len(portfolio_analysis))
    
    # Detailed Fund Analysis
    st.subheader("Fund-wise Performance")
    
    # Prepare data for detailed table
    fund_details = []
    for fund in portfolio_analysis:
        fund_details.append({
            'Scheme Code': fund['scheme_code'],
            'Scheme Name': fund['scheme_name'],
            'Units': round(fund['current_units'], 4),
            'Total Invested': round(fund['total_invested'], 2),
            'Latest NAV': round(fund['latest_nav'], 2) if fund['latest_nav'] else 'N/A',
            'Current Value': round(fund['current_value'], 2),
            'Gain/Loss': round(fund['current_value'] - fund['total_invested'], 2),
            'Return %': round((fund['current_value'] - fund['total_invested']) / fund['total_invested'] * 100, 2) if fund['total_invested'] else 0
        })
    
    # Display fund details
    fund_df = pd.DataFrame(fund_details)
    st.dataframe(fund_df, use_container_width=True)
    
    # Corpus Value Over Time Visualization
    st.subheader("Cumulative Corpus Value Over Time")
    
    # Prepare dropdown options for fund selection
    fund_options = ['All Funds'] + [f"{fund['scheme_name']} ({fund['scheme_code']})" for fund in portfolio_analysis]
    selected_fund = st.selectbox("Select Fund to Visualize", fund_options)
    
    # Prepare data for plotting
    fig = go.Figure()
    
    for fund in portfolio_analysis:
        # Create DataFrame for this fund's transactions
        df = pd.DataFrame(fund['transactions'])
        
        # Calculate corpus value at each point
        if fund['latest_nav']:
            df['corpus_value'] = df['cumulative_units'] * fund['latest_nav']
            
            # Fund name for display
            fund_display_name = f"{fund['scheme_name']} ({fund['scheme_code']})"
            
            # Plotting logic based on fund selection
            if selected_fund == 'All Funds' or selected_fund == fund_display_name:
                # Plot line for this fund
                fig.add_trace(go.Scatter(
                    x=df['date'], 
                    y=df['corpus_value'], 
                    mode='lines+markers',
                    name=fund_display_name
                ))
    
    # Customize layout
    fig.update_layout(
        title=f'Cumulative Corpus Value Over Time: {selected_fund}',
        xaxis_title='Date',
        yaxis_title='Corpus Value (₹)',
        height=600,
        hovermode='x unified'
    )
    
    # Display the plot
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
