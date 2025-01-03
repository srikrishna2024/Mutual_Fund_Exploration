import streamlit as st
import pandas as pd
import numpy as np
import psycopg
import plotly.graph_objs as go

class MutualFundPerformanceAnalyzer:
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

    def get_fund_categories(self):
        """
        Fetch unique fund categories from mutual_fund_master_data
        """
        conn = self.get_db_connection()
        if not conn:
            return []
        
        try:
            query = """
            SELECT DISTINCT category 
            FROM mutual_fund_master_data 
            WHERE category IS NOT NULL 
            ORDER BY category
            """
            df = pd.read_sql(query, conn)
            return list(df['category'])
        except Exception as e:
            st.error(f"Error fetching fund categories: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def fetch_fund_nav_data(self, category, start_date, end_date):
        """
        Fetch NAV history for funds in a specific category
        """
        conn = self.get_db_connection()
        if not conn:
            return None
        
        try:
            query = """
            SELECT 
                nav.scheme_code, 
                mfd.scheme_name, 
                nav.date, 
                nav.net_asset_value
            FROM mutual_fund_nav nav
            JOIN mutual_fund_master_data mfd ON nav.scheme_code = mfd.scheme_code
            WHERE mfd.category = %s::text 
              AND nav.date >= %s::date 
              AND nav.date <= %s::date
            ORDER BY nav.scheme_code, nav.date
            """
            df = pd.read_sql(query, conn, params=(category, start_date, end_date))
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['net_asset_value'] = pd.to_numeric(df['net_asset_value'], errors='coerce')
            return df
        except Exception as e:
            st.error(f"Error fetching NAV data for {category}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def calculate_rolling_returns(self, nav_df, rolling_period):
        """
        Calculate rolling returns for each fund
        """
        # Group by scheme code and calculate rolling returns
        rolling_returns = nav_df.groupby('scheme_code').apply(
            lambda x: x.set_index('date')['net_asset_value']
               .rolling(window=rolling_period)
               .apply(lambda y: (y.iloc[-1] / y.iloc[0] - 1) * 100)
               .reset_index()
        ).reset_index()
        
        # Rename columns and merge back scheme names
        rolling_returns.columns = ['scheme_code', 'date', 'rolling_return']
        rolling_returns = rolling_returns.merge(
            nav_df[['scheme_code', 'scheme_name']].drop_duplicates(), 
            on='scheme_code'
        )
        
        # Get the last rolling return for each scheme
        last_rolling_returns = rolling_returns.loc[rolling_returns.groupby('scheme_code')['date'].idxmax()]
        
        return last_rolling_returns

    def calculate_maximum_drawdown(self, nav_df, period):
        """
        Calculate maximum drawdown percentage for each fund
        """
        def max_drawdown(series):
            cumulative = (series / series.iloc[0] - 1) * 100
            max_drop = (cumulative.cummax() - cumulative).max()
            return max_drop

        # Group by scheme code and calculate max drawdown
        max_drawdowns = nav_df.groupby('scheme_code').apply(
            lambda x: max_drawdown(x.set_index('date')['net_asset_value'])
        ).reset_index()
        
        max_drawdowns.columns = ['scheme_code', 'max_drawdown']
        
        # Merge scheme names
        max_drawdowns = max_drawdowns.merge(
            nav_df[['scheme_code', 'scheme_name']].drop_duplicates(), 
            on='scheme_code'
        )
        
        return max_drawdowns

def main():
    st.set_page_config(page_title="Mutual Fund Performance Heatmap", page_icon="📊", layout="wide")
    analyzer = MutualFundPerformanceAnalyzer()
    st.title("📈 Mutual Fund Performance Heatmap")

    # Columns for input selection
    col1, col2, col3 = st.columns(3)

    # Select fund category
    with col1:
        fund_categories = analyzer.get_fund_categories()
        selected_category = st.selectbox("Select Fund Category", ['Select Category'] + fund_categories)

    # Select rolling returns period
    with col2:
        period_options = [30, 60, 90, 180, 365]
        selected_period = st.selectbox("Select Rolling Returns Period (Days)", period_options)

    # Date range selection
    with col3:
        # Get current date and 3 years back
        end_date = pd.Timestamp.now().date()
        start_date = end_date - pd.DateOffset(years=3)
        
        st.write("Performance Analysis Period:")
        start_date = st.date_input("Start Date", start_date)
        end_date = st.date_input("End Date", end_date)

    # Analyze button
    analyze_button = st.button("Generate Performance Heatmap")

    # Performance analysis
    if analyze_button:
        if selected_category == 'Select Category':
            st.warning("Please select a fund category.")
            return

        # Fetch NAV data
        nav_data = analyzer.fetch_fund_nav_data(selected_category, start_date, end_date)
        
        if nav_data is None or nav_data.empty:
            st.error("No data available for the selected category and period.")
            return

        # Calculate rolling returns and max drawdown
        rolling_returns = analyzer.calculate_rolling_returns(nav_data, selected_period)
        max_drawdowns = analyzer.calculate_maximum_drawdown(nav_data, (end_date - start_date).days)

        # Merge rolling returns and max drawdowns
        performance_df = rolling_returns.merge(max_drawdowns, on=['scheme_code', 'scheme_name'])
        
        # Remove any rows with NaN
        performance_df = performance_df.dropna()

        if performance_df.empty:
            st.error("Could not calculate performance metrics.")
            return

        # Create scatter plot
        fig = go.Figure(data=go.Scatter(
            x=performance_df['rolling_return'],
            y=performance_df['max_drawdown'],
            mode='markers+text',
            marker=dict(
                size=10,
                color=performance_df['rolling_return'],  # color by rolling returns
                colorscale='Viridis',
                colorbar=dict(title='Rolling Returns %'),
                showscale=True
            ),
            text=performance_df['scheme_name'],
            textposition='top center'
        ))

        # Update layout
        fig.update_layout(
            title=f'Fund Performance: Rolling Returns vs Max Drawdown ({selected_category})',
            xaxis_title=f'Rolling Returns % ({selected_period} Days)',
            yaxis_title='Maximum Drawdown % (3 Years)',
            height=600,
            width=1000
        )

        # Display the plot
        st.plotly_chart(fig, use_container_width=True)

        # Display performance metrics table
        st.subheader("Performance Metrics")
        st.dataframe(performance_df[['scheme_name', 'rolling_return', 'max_drawdown']])

if __name__ == "__main__":
    main()
