import streamlit as st
import pandas as pd
import numpy as np
import psycopg
import plotly.graph_objs as go

class MutualFundRiskReturnsAnalyzer:
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

    def calculate_fund_metrics(self, category, time_period):
        """
        Calculate XIRR and Standard Deviation for funds and benchmark
        """
        conn = self.get_db_connection()
        if not conn:
            return None

        try:
            # Determine date range based on time period
            if time_period == 'YTD':
                start_date = pd.Timestamp.now().replace(month=1, day=1).date()
            elif time_period == '1 year':
                start_date = pd.Timestamp.now() - pd.DateOffset(years=1)
            elif time_period == '2 years':
                start_date = pd.Timestamp.now() - pd.DateOffset(years=2)
            elif time_period == '3 years':
                start_date = pd.Timestamp.now() - pd.DateOffset(years=3)
            else:  # max
                start_date = pd.Timestamp.min.date()

            end_date = pd.Timestamp.now().date()

            # Updated SQL query
            query = """
            WITH fund_nav AS (
                SELECT 
                    md.scheme_code, 
                    md.scheme_name, 
                    md.category,
                    nav.date, 
                    nav.net_asset_value
                FROM mutual_fund_nav nav
                JOIN mutual_fund_master_data md ON nav.scheme_code = md.scheme_code
                WHERE md.category = %(category)s 
                  AND nav.date >= %(start_date)s 
                  AND nav.date <= %(end_date)s
            ), benchmark_nav AS (
                SELECT 
                    date, 
                    price
                FROM benchmark_index
                WHERE date >= %(start_date)s 
                  AND date <= %(end_date)s
            ), daily_fund_returns AS (
                SELECT 
                    scheme_code,
                    date,
                    (net_asset_value / LAG(net_asset_value) OVER (PARTITION BY scheme_code ORDER BY date) - 1) * 100 AS daily_return,
                    net_asset_value
                FROM fund_nav
            ), daily_benchmark_returns AS (
                SELECT 
                    date,
                    (price / LAG(price) OVER (ORDER BY date) - 1) * 100 AS daily_return,
                    price
                FROM benchmark_nav
            ), fund_aggregates AS (
                SELECT 
                    scheme_code,
                    MAX(net_asset_value) / MIN(net_asset_value) - 1 AS fund_return,
                    COALESCE(STDDEV(daily_return), 0) AS fund_std_dev
                FROM daily_fund_returns
                GROUP BY scheme_code
            ), benchmark_aggregates AS (
                SELECT 
                    MAX(price) / MIN(price) - 1 AS benchmark_return,
                    COALESCE(STDDEV(daily_return), 0) AS benchmark_std_dev
                FROM daily_benchmark_returns
            )
            SELECT 
                fa.scheme_code,
                mf.scheme_name,
                COALESCE(fa.fund_return, 0) * 100 AS fund_return,
                COALESCE(fa.fund_std_dev, 0) AS fund_std_dev,
                COALESCE(ba.benchmark_return, 0) * 100 AS benchmark_return,
                COALESCE(ba.benchmark_std_dev, 0) AS benchmark_std_dev,
                COALESCE(fa.fund_return - ba.benchmark_return, 0) * 100 AS xirr_diff,
                COALESCE(fa.fund_std_dev - ba.benchmark_std_dev, 0) AS std_dev_diff
            FROM fund_aggregates fa
            JOIN mutual_fund_master_data mf ON fa.scheme_code = mf.scheme_code
            CROSS JOIN benchmark_aggregates ba;
            """

            # Prepare parameters dictionary
            params = {
                'category': category,
                'start_date': start_date,
                'end_date': end_date
            }

            # Log parameters for debugging
            st.info(f"Query Parameters: {params}")

            # Execute the query
            df = pd.read_sql(query, conn, params=params)
            
            return df

        except Exception as e:
            st.error(f"Error calculating fund metrics: {e}")
            st.error(f"Category: {category}")
            st.error(f"Start Date: {start_date}")
            st.error(f"End Date: {end_date}")
            return None
        finally:
            if conn:
                conn.close()

def main():
    st.set_page_config(page_title="Mutual Fund Risk vs Returns", page_icon="📊", layout="wide")
    analyzer = MutualFundRiskReturnsAnalyzer()
    st.title("📈 Mutual Fund Risk vs Returns Quadrant Analysis")

    # Columns for input selection
    col1, col2, col3 = st.columns(3)

    # Select fund category
    with col1:
        fund_categories = analyzer.get_fund_categories()
        selected_category = st.selectbox("Select Fund Category", ['Select Category'] + fund_categories)

    # Select time period
    with col2:
        time_periods = ['YTD', '1 year', '2 years', '3 years', 'max']
        selected_time_period = st.selectbox("Select Time Period", time_periods)

    # Analyze button
    analyze_button = st.button("Generate Risk vs Returns Quadrant")

    if analyze_button:
        if selected_category == 'Select Category':
            st.warning("Please select a fund category.")
            return

        # Calculate fund metrics
        fund_metrics = analyzer.calculate_fund_metrics(selected_category, selected_time_period)
        
        if fund_metrics is None or fund_metrics.empty:
            st.error("Could not calculate performance metrics.")
            return

        # Create quadrant scatter plot
        fig = go.Figure(data=go.Scatter(
            x=fund_metrics['xirr_diff'],
            y=fund_metrics['std_dev_diff'],
            mode='markers',
            marker=dict(
                size=10,
                color=fund_metrics['xirr_diff'],
                colorscale='Viridis',
                colorbar=dict(title='XIRR Difference %'),
                showscale=True
            ),
            hovertemplate='<b>Fund Name</b>: %{text}<br>' + 
                          '<b>XIRR Difference</b>: %{x:.2f}%<br>' + 
                          '<b>Std Dev Difference</b>: %{y:.2f}%<extra></extra>',
            text=fund_metrics['scheme_name']
        ))

        # Add horizontal and vertical lines to create quadrants
        fig.add_shape(type="line", x0=0, x1=0, y0=-100, y1=100, 
                      line=dict(color="Red", width=2, dash="dash"))
        fig.add_shape(type="line", x0=-100, x1=100, y0=0, y1=0, 
                      line=dict(color="Red", width=2, dash="dash"))

        # Update layout
        fig.update_layout(
            title=f'Fund Performance Quadrants: {selected_category} ({selected_time_period})',
            xaxis_title='Excess XIRR % (Fund - Benchmark)',
            yaxis_title='Excess Standard Deviation % (Fund - Benchmark)',
            height=600,
            width=1000
        )

        st.plotly_chart(fig, use_container_width=True)

        # Display performance metrics table
        st.subheader("Performance Metrics")
        st.dataframe(fund_metrics[['scheme_name', 'fund_return', 'fund_std_dev', 
                                   'benchmark_return', 'benchmark_std_dev', 
                                   'xirr_diff', 'std_dev_diff']])

if __name__ == "__main__":
    main()
