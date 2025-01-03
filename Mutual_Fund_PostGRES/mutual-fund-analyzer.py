import streamlit as st
import pandas as pd
import psycopg
import plotly.express as px
import plotly.graph_objs as go
import numpy as np
from datetime import datetime

class MutualFundAnalyzer:
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

    def fetch_fund_details(self, conn, scheme_code):
        """Fetch basic fund details"""
        query = """
        SELECT scheme_code, scheme_name 
        FROM mutual_fund_master_data 
        WHERE scheme_code = %s
        """
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, (scheme_code,))
            return cur.fetchone()

    def fetch_nav_history(self, conn, scheme_code):
        """Fetch NAV history for a specific fund"""
        query = """
        SELECT date, net_asset_value 
        FROM mutual_fund_nav 
        WHERE scheme_code = %s 
        ORDER BY date
        """
        return pd.read_sql(query, conn, params=(scheme_code,))

    def fetch_performance_metrics(self, conn, scheme_code):
        """Fetch performance metrics for a specific fund"""
        query = """
        SELECT 
            monthly_mean_return, monthly_return_volatility,
            quarterly_mean_return, quarterly_return_volatility,
            yearly_mean_return, yearly_return_volatility
        FROM mutual_fund_performance_metrics 
        WHERE scheme_code = %s
        """
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, (scheme_code,))
            return cur.fetchone()

    def calculate_rolling_returns_and_std(self, conn, scheme_code):
        """
        Calculate comprehensive rolling returns and standard deviation for different periods
        
        Args:
            conn: Database connection
            scheme_code: Mutual fund scheme code
        
        Returns:
            Dictionary of DataFrames with rolling returns and standard deviation
        """
        query = """
        SELECT date, net_asset_value 
        FROM mutual_fund_nav 
        WHERE scheme_code = %s 
        ORDER BY date
        """
        
        # Fetch NAV data
        nav_data = pd.read_sql(query, conn, params=(scheme_code,))
        
        # Ensure date column is datetime and sorted
        nav_data['date'] = pd.to_datetime(nav_data['date'])
        nav_data = nav_data.sort_values('date')
        
        # Ensure enough data
        if len(nav_data) < 5*365:  # At least 5 years of data
            st.warning("Insufficient data for comprehensive rolling returns analysis")
            return None
        
        # Define rolling periods in days
        rolling_periods = {
            '1 Year': 365,
            '2 Years': 365*2,
            '3 Years': 365*3,
            '5 Years': 365*5
        }
        
        # Store results for each period
        rolling_results = {}
        
        for period_name, days in rolling_periods.items():
            # Prepare a list to store rolling data
            rolling_data = []
            
            # Calculate rolling returns
            for start_idx in range(len(nav_data)):
                start_date = nav_data.iloc[start_idx]['date']
                
                # Find the index of the last date within the period
                end_idx = nav_data[
                    (nav_data['date'] <= start_date + pd.Timedelta(days=days)) & 
                    (nav_data.index > start_idx)
                ].index
                
                # If no suitable end date, skip this iteration
                if len(end_idx) == 0:
                    continue
                end_idx = end_idx[-1]
                
                # Calculate returns
                start_nav = nav_data.iloc[start_idx]['net_asset_value']
                end_nav = nav_data.iloc[end_idx]['net_asset_value']
                
                # Calculate years
                years = (nav_data.iloc[end_idx]['date'] - start_date).days / 365
                
                # Calculate CAGR
                if years > 0:
                    cagr = min(((end_nav / start_nav) ** (1/years) - 1) * 100, 100)
                    
                    # Calculate returns for standard deviation
                    period_returns = nav_data.loc[start_idx:end_idx, 'net_asset_value'].pct_change().dropna()
                    rolling_std = min(period_returns.std() * 100, 2.0)  # Convert to percentage, cap at 2%
                    
                    rolling_data.append({
                        'Start Date': start_date.date(),
                        'End Date': nav_data.iloc[end_idx]['date'].date(),
                        'CAGR (%)': round(cagr, 2),
                        'Rolling Std Dev (%)': round(rolling_std, 2)
                    })
            
            # Convert to DataFrame
            rolling_results[period_name] = pd.DataFrame(rolling_data)
        
        return rolling_results

def main():
    st.set_page_config(page_title="Mutual Fund Analyzer", page_icon="💹", layout="wide")
    
    st.title("🔍 Mutual Fund Performance Analyzer")
    
    # Initialize analyzer
    analyzer = MutualFundAnalyzer()
    
    # Database connection
    conn = analyzer.get_db_connection()
    if not conn:
        st.error("Could not establish database connection.")
        return
    
    try:
        # Fund Selection
        st.sidebar.header("Fund Selection")
        
        # Direct scheme code input
        scheme_code = st.sidebar.text_input("Enter Scheme Code")
        
        # Only proceed if a valid scheme_code is selected
        if scheme_code:
            # Fetch fund details
            fund_details = analyzer.fetch_fund_details(conn, scheme_code)
            if not fund_details:
                st.error("Fund not found!")
                return
            
            # Display fund basic information
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Scheme Name", fund_details['scheme_name'])
            with col2:
                st.metric("Scheme Code", scheme_code)
            
            # Performance Metrics
            performance_metrics = analyzer.fetch_performance_metrics(conn, scheme_code)
            
            # Create tabs for different views
            tab1, tab2, tab3 = st.tabs(["NAV History", "Rolling Performance", "Performance Metrics"])
            
            with tab1:
                # NAV History Plot
                nav_history = analyzer.fetch_nav_history(conn, scheme_code)
                if not nav_history.empty:
                    fig = px.line(nav_history, x='date', y='net_asset_value', 
                                   title=f"NAV History for {fund_details['scheme_name']}")
                    st.plotly_chart(fig, use_container_width=True)
                
            with tab2:
                # Rolling Returns and Standard Deviation
                rolling_data = analyzer.calculate_rolling_returns_and_std(conn, scheme_code)
                
                if rolling_data is not None:
                    # Color palette
                    colors = {
                        '1 Year': ['blue', 'lightblue'],
                        '2 Years': ['green', 'lightgreen'],
                        '3 Years': ['red', 'lightcoral'],
                        '5 Years': ['purple', 'lavender']
                    }
                    
                    # Create a figure with subplots vertically
                    for period, data in rolling_data.items():
                        # Create figure with secondary y-axis
                        fig = go.Figure()
                        
                        # Add CAGR trace to primary y-axis
                        fig.add_trace(
                            go.Scatter(
                                x=data['End Date'], 
                                y=data['CAGR (%)'], 
                                mode='lines', 
                                name=f'{period} CAGR',
                                line=dict(color=colors[period][0]),
                                yaxis='y1'
                            )
                        )
                        
                        # Add Rolling Std Dev trace to secondary y-axis
                        fig.add_trace(
                            go.Scatter(
                                x=data['End Date'], 
                                y=data['Rolling Std Dev (%)'], 
                                mode='lines', 
                                name=f'{period} Rolling Std Dev',
                                line=dict(color=colors[period][1], dash='dot'),
                                yaxis='y2'
                            )
                        )
                        
                        # Update layout with two y-axes
                        fig.update_layout(
                            title_text=f'Rolling Performance - {period}',
                            xaxis_title='Date',
                            height=400,
                            yaxis=dict(
                                title='CAGR (%)',
                                range=[0, 100]
                            ),
                            yaxis2=dict(
                                title='Rolling Std Dev (%)',
                                anchor='x',
                                overlaying='y',
                                side='right',
                                range=[0, 2.0]  # Changed from [0, 100] to [0, 2.0]
                            ),
                            legend_title_text='Metrics'
                        )
                        
                        # Display the plot
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Display tabular data
                    st.subheader("Detailed Rolling Returns and Volatility Data")
                    for period, data in rolling_data.items():
                        st.write(f"**{period} Data**")
                        st.dataframe(data, use_container_width=True)
                
            with tab3:
                # Performance Metrics
                if performance_metrics:
                    st.subheader("Performance Metrics")
                    metric_columns = st.columns(2)
                    
                    metrics_to_display = [
                        ('Monthly Mean Return', 'monthly_mean_return'),
                        ('Monthly Return Volatility', 'monthly_return_volatility'),
                        ('Quarterly Mean Return', 'quarterly_mean_return'),
                        ('Quarterly Return Volatility', 'quarterly_return_volatility'),
                        ('Yearly Mean Return', 'yearly_mean_return'),
                        ('Yearly Return Volatility', 'yearly_return_volatility')
                    ]
                    
                    for i, (label, key) in enumerate(metrics_to_display):
                        with metric_columns[i % 2]:
                            st.metric(label, f"{performance_metrics.get(key, 'N/A')}%")
        else:
            # Initial welcome message or instructions
            st.info("Please enter a scheme code to analyze mutual fund performance.")
    
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
