import streamlit as st
import pandas as pd
import psycopg
import plotly.graph_objs as go
import numpy as np
from datetime import datetime, timedelta
from scipy.optimize import newton

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
            df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
            return df
        except Exception as e:
            st.error(f"Error fetching portfolio transactions: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def fetch_fund_nav_history(self, scheme_code, start_date, end_date):
        """
        Fetch NAV history for a specific fund within a date range
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            # Ensure dates are converted to strings in 'YYYY-MM-DD' format if they're datetime
            start_date = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, pd.Timestamp)) else start_date
            end_date = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, pd.Timestamp)) else end_date
            
            query = """
            SELECT date, net_asset_value
            FROM mutual_fund_nav
            WHERE scheme_code = %s::text 
              AND date >= %s::date 
              AND date <= %s::date
            ORDER BY date
            """
            df = pd.read_sql(query, conn, params=(scheme_code, start_date, end_date))
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['net_asset_value'] = pd.to_numeric(df['net_asset_value'], errors='coerce')
            df = df.dropna(subset=['net_asset_value'])
            return df
        except Exception as e:
            st.error(f"Error fetching NAV history for {scheme_code}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_latest_nav_for_funds(self, scheme_codes):
        """
        Fetch the latest NAV for given scheme codes
        """
        conn = self.get_db_connection()
        if not conn:
            return {}
        
        try:
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
            latest_navs['latest_nav_date'] = pd.to_datetime(latest_navs['latest_nav_date']).dt.date
            return dict(zip(latest_navs['scheme_code'], 
                            zip(latest_navs['latest_nav'], 
                                latest_navs['latest_nav_date'])))
        except Exception as e:
            st.error(f"Error fetching latest NAV: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def get_benchmark_index_data(self, indexname, start_date=None, end_date=None):
        """
        Fetch benchmark index data for a given index name.
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            # Ensure dates are converted to strings in 'YYYY-MM-DD' format if they're datetime
            start_date = start_date.strftime('%Y-%m-%d') if isinstance(start_date, (datetime, pd.Timestamp)) else start_date
            end_date = end_date.strftime('%Y-%m-%d') if isinstance(end_date, (datetime, pd.Timestamp)) else end_date
            
            # Query to fetch benchmark data
            if start_date and end_date:
                query = """
                SELECT date, price 
                FROM benchmark_index 
                WHERE indexname = %s::text 
                  AND date >= %s::date 
                  AND date <= %s::date
                ORDER BY date
                """
                df = pd.read_sql(query, conn, params=(indexname, start_date, end_date))
            elif start_date:
                query = """
                SELECT date, price 
                FROM benchmark_index 
                WHERE indexname = %s::text AND date >= %s::date
                ORDER BY date
                """
                df = pd.read_sql(query, conn, params=(indexname, start_date))
            else:
                query = """
                SELECT date, price 
                FROM benchmark_index 
                WHERE indexname = %s::text
                ORDER BY date
                """
                df = pd.read_sql(query, conn, params=(indexname,))
            
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df.dropna(subset=['price'])
            return df
        except Exception as e:
            st.error(f"Error fetching benchmark index data: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def calculate_fund_performance_metrics(self, benchmark_name, period_option):
        """
        Calculate XIRR, standard deviation, and other performance metrics for each fund
        """
        # Fetch portfolio transactions
        transactions = self.fetch_portfolio_transactions()
        if transactions is None:
            return None

        # Determine date range based on period option
        today = datetime.now().date()
        if period_option == 'Year to Date':
            start_date = datetime(today.year, 1, 1).date()
        elif period_option == '1 Year':
            start_date = today - timedelta(days=365)
        elif period_option == '2 Years':
            start_date = today - timedelta(days=2*365)
        elif period_option == '3 Years':
            start_date = today - timedelta(days=3*365)
        elif period_option == 'Maximum':
            # Ensure we convert to date if it's a datetime
            start_date = pd.to_datetime(transactions['transaction_date'].min()).date()
        else:
            st.error("Invalid period selected")
            return None

        # Get latest NAVs for funds
        scheme_codes = transactions['scheme_code'].unique().tolist()
        latest_navs = self.get_latest_nav_for_funds(scheme_codes)

        # Performance metrics storage
        performance_metrics = []

        # Group transactions by scheme
        grouped = transactions.groupby('scheme_code')

        for scheme_code, scheme_group in grouped:
            # Filter transactions within the selected period
            scheme_group_filtered = scheme_group[scheme_group['transaction_date'] >= start_date]
            
            # Skip if no transactions in the selected period
            if scheme_group_filtered.empty:
                continue

            # Sort transactions chronologically
            scheme_group_sorted = scheme_group_filtered.sort_values('transaction_date')
            
            # Transaction details
            invest_transactions = scheme_group_sorted[scheme_group_sorted['transaction_type'] == 'Invest']
            redeem_transactions = scheme_group_sorted[scheme_group_sorted['transaction_type'] == 'Redeem']

            # Calculate XIRR
            cash_flows = []
            for _, invest in invest_transactions.iterrows():
                cash_flows.append({
                    'date': invest['transaction_date'],
                    'amount': -invest['amount']  # Negative for investment
                })
            
            # Add final redemption / current value
            latest_nav_info = latest_navs.get(scheme_code, (None, None))
            latest_nav, latest_nav_date = latest_nav_info
            
            if latest_nav is not None and latest_nav_date >= start_date:
                current_units = invest_transactions['units'].sum() - (redeem_transactions['units'].sum() if not redeem_transactions.empty else 0)
                current_value = current_units * latest_nav
                
                cash_flows.append({
                    'date': latest_nav_date,
                    'amount': current_value
                })

                # XIRR Calculation
                def xnpv(rate, cash_flows):
                    return sum([
                        cf['amount'] / (1 + rate) ** ((cf['date'] - cash_flows[0]['date']).days / 365)
                        for cf in cash_flows
                    ])

                def xirr(cash_flows, guess=0.1):
                    try:
                        return newton(lambda r: xnpv(r, cash_flows), guess)
                    except:
                        return None

                xirr_value = xirr(cash_flows)
                
                # Calculate standard deviation for the specific fund
                std_dev = None
                nav_history = self.fetch_fund_nav_history(scheme_code, start_date, today)
                
                if nav_history is not None and len(nav_history) > 1:
                    # Calculate daily returns
                    nav_history['daily_return'] = nav_history['net_asset_value'].pct_change()
                    std_dev = nav_history['daily_return'].dropna().std() * np.sqrt(252)  # Annualized

                performance_metrics.append({
                    'scheme_code': scheme_code,
                    'scheme_name': scheme_group.iloc[0]['scheme_name'],
                    'xirr': xirr_value * 100 if xirr_value is not None else None,
                    'standard_deviation': std_dev * 100 if std_dev is not None else None,
                    'current_value': current_value,
                    'total_invested': invest_transactions['amount'].sum()
                })

        return performance_metrics

    def get_available_benchmark_indices(self):
        """
        Fetch list of available benchmark indices
        """
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            query = """
            SELECT DISTINCT indexname 
            FROM benchmark_index 
            ORDER BY indexname
            """
            df = pd.read_sql(query, conn)
            return list(df['indexname'])
        except Exception as e:
            st.error(f"Error fetching benchmark indices: {e}")
            return []
        finally:
            if conn:
                conn.close()

def main():
    st.set_page_config(page_title="Mutual Fund Performance Analysis", page_icon="📊", layout="wide")
    analyzer = PortfolioAnalyzer()
    st.title("📈 Mutual Fund Performance Scatter Plot")

    # Columns for input selection
    col1, col2, col3 = st.columns(3)

    # Select benchmark
    with col1:
        benchmark_indices = analyzer.get_available_benchmark_indices()
        selected_benchmark = st.selectbox("Select Benchmark Index", ['Select Benchmark'] + benchmark_indices)

    # Select period
    with col2:
        period_options = ['Year to Date', '1 Year', '2 Years', '3 Years', 'Maximum']
        selected_period = st.selectbox("Select Performance Period", period_options)

    # Analyze button
    with col3:
        analyze_button = st.button("Generate Performance Analysis")

    # Performance analysis
    if analyze_button:
        if selected_benchmark == 'Select Benchmark':
            st.warning("Please select a benchmark index.")
            return

        # Calculate performance metrics
        performance_metrics = analyzer.calculate_fund_performance_metrics(selected_benchmark, selected_period)
        
        if performance_metrics is None or len(performance_metrics) == 0:
            st.error("Could not retrieve performance data. Check your data or selected period.")
            return

        # Prepare data for scatter plot
        df_performance = pd.DataFrame(performance_metrics)
        df_performance = df_performance.dropna(subset=['xirr', 'standard_deviation'])

        if df_performance.empty:
            st.error("No performance data available for the selected period and benchmark.")
            return

        # Create scatter plot
        fig = go.Figure()

        # Add scatter plot traces
        fig.add_trace(go.Scatter(
            x=df_performance['xirr'],
            y=df_performance['standard_deviation'],
            mode='markers+text',
            marker=dict(
                size=10,
                color=df_performance['xirr'],  # color by XIRR
                colorscale='Viridis',
                colorbar=dict(title='XIRR %'),
                showscale=True
            ),
            text=df_performance['scheme_name'],
            textposition='top center'
        ))

        # Update layout
        fig.update_layout(
            title=f'Fund Performance: XIRR vs Standard Deviation ({selected_benchmark}) - {selected_period}',
            xaxis_title='XIRR (%)',
            yaxis_title='Annualized Standard Deviation (%)',
            height=600,
            width=1000
        )

        # Display the plot
        st.plotly_chart(fig, use_container_width=True)

        # Display performance metrics table
        st.subheader("Performance Metrics")
        st.dataframe(df_performance[['scheme_name', 'xirr', 'standard_deviation', 'current_value', 'total_invested']])

if __name__ == "__main__":
    main()
