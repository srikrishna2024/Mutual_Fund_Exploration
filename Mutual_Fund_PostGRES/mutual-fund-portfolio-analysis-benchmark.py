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
            return dict(zip(latest_navs['scheme_code'], 
                            zip(latest_navs['latest_nav'], 
                                latest_navs['latest_nav_date'])))
        except Exception as e:
            st.error(f"Error fetching latest NAV: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def get_benchmark_index_data(self, indexname, start_date=None):
        """
        Fetch benchmark index data for a given index name.
        
        Args:
            indexname (str): Name of the benchmark index.
            start_date (datetime or str, optional): Start date for filtering data.
        
        Returns:
            pandas DataFrame with benchmark index values.
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            # Ensure start_date is converted to a string in 'YYYY-MM-DD' format if it's a datetime
            if isinstance(start_date, datetime):
                start_date = start_date.strftime('%Y-%m-%d')
            
            # Query to fetch benchmark data
            if start_date:
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
            
            df['date'] = pd.to_datetime(df['date'])
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df.dropna(subset=['price'])
            return df
        except Exception as e:
            st.error(f"Error fetching benchmark index data: {e}")
            return None
        finally:
            if conn:
                conn.close()

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

    def analyze_portfolio(self):
        """
        Comprehensive portfolio analysis
        """
        transactions = self.fetch_portfolio_transactions()
        if transactions is None:
            return None
        
        grouped = transactions.groupby('scheme_code')
        portfolio_analysis = []
        scheme_codes = transactions['scheme_code'].unique().tolist()
        latest_navs = self.get_latest_nav_for_funds(scheme_codes)
        
        for scheme_code, scheme_group in grouped:
            cumulative_transactions = []
            current_units = 0
            total_invested = 0
            oldest_investment_date = None
            
            scheme_group_sorted = scheme_group.sort_values('transaction_date')
            
            for _, transaction in scheme_group_sorted.iterrows():
                if transaction['transaction_type'] == 'Invest':
                    current_units += transaction['units']
                    total_invested += transaction['amount']
                    if oldest_investment_date is None or transaction['transaction_date'] < oldest_investment_date:
                        oldest_investment_date = transaction['transaction_date']
                elif transaction['transaction_type'] == 'Redeem':
                    current_units -= transaction['units']
                
                cumulative_transactions.append({
                    'date': transaction['transaction_date'],
                    'cumulative_units': current_units,
                    'total_invested': total_invested
                })
            
            latest_nav_info = latest_navs.get(scheme_code, (None, None))
            latest_nav, latest_nav_date = latest_nav_info
            
            portfolio_analysis.append({
                'scheme_code': scheme_code,
                'scheme_name': scheme_group.iloc[0]['scheme_name'],
                'current_units': current_units,
                'total_invested': total_invested,
                'latest_nav': latest_nav,
                'latest_nav_date': latest_nav_date,
                'current_value': current_units * (latest_nav or 0),
                'transactions': cumulative_transactions,
                'oldest_investment_date': oldest_investment_date
            })
        
        return portfolio_analysis

def main():
    st.set_page_config(page_title="Mutual Fund Portfolio Analysis", page_icon="📊", layout="wide")
    analyzer = PortfolioAnalyzer()
    st.title("🚀 Mutual Fund Portfolio Analysis")
    
    portfolio_analysis = analyzer.analyze_portfolio()
    if portfolio_analysis is None:
        st.error("Could not retrieve portfolio data.")
        return
    
    tab1, tab2 = st.tabs(["Portfolio Overview", "Benchmark Comparison"])
    
    with tab1:
        st.subheader("Portfolio Summary")
        total_invested = sum(fund['total_invested'] for fund in portfolio_analysis)
        total_current_value = sum(fund['current_value'] for fund in portfolio_analysis)
        total_gain_loss = total_current_value - total_invested
        total_return_percentage = (total_gain_loss / total_invested * 100) if total_invested else 0
        
        st.metric("Total Invested", f"₹{total_invested:,.2f}")
        st.metric("Current Portfolio Value", f"₹{total_current_value:,.2f}")
        st.metric("Total Gain/Loss", f"₹{total_gain_loss:,.2f}")
    
    with tab2:
        st.subheader("Cumulative Corpus Value Analysis")
        fund_options = ['Select Fund'] + [f"{fund['scheme_name']} ({fund['scheme_code']})" for fund in portfolio_analysis]
        selected_fund = st.selectbox("Select Fund", fund_options)
        benchmark_indices = analyzer.get_available_benchmark_indices()
        selected_benchmark = st.selectbox("Select Benchmark Index", ['Select Benchmark'] + benchmark_indices)
        
        if st.button("Generate Comparative Analysis") and selected_fund != 'Select Fund' and selected_benchmark != 'Select Benchmark':
            selected_fund_code = selected_fund.split('(')[-1].strip(')')
            selected_fund_details = next(
                (fund for fund in portfolio_analysis if fund['scheme_code'] == selected_fund_code), 
                None
            )
            
            if selected_fund_details:
                oldest_investment_date = selected_fund_details['oldest_investment_date']
                benchmark_data = analyzer.get_benchmark_index_data(selected_benchmark, start_date=oldest_investment_date)
                
                if benchmark_data is not None and not benchmark_data.empty:
                    initial_investment = selected_fund_details['total_invested']
                    initial_benchmark_value = benchmark_data.iloc[0]['price']
                    benchmark_data['benchmark_corpus'] = (
                        benchmark_data['price'] / initial_benchmark_value * initial_investment
                    )
                    
                    df_fund = pd.DataFrame(selected_fund_details['transactions'])
                    df_fund['corpus_value'] = df_fund['cumulative_units'] * selected_fund_details['latest_nav']
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df_fund['date'], 
                        y=df_fund['corpus_value'], 
                        mode='lines+markers',
                        name=f"{selected_fund_details['scheme_name']} (Fund)"
                    ))
                    fig.add_trace(go.Scatter(
                        x=benchmark_data['date'], 
                        y=benchmark_data['benchmark_corpus'], 
                        mode='lines+markers',
                        name=f"{selected_benchmark} (Benchmark)"
                    ))
                    fig.update_layout(
                        title=f'Comparative Corpus Value: {selected_fund} vs {selected_benchmark}',
                        xaxis_title='Date',
                        yaxis_title='Corpus Value (₹)',
                        height=600,
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
