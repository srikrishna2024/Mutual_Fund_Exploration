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
            
            # Ensure scheme_code is converted to string
            df['scheme_code'] = df['scheme_code'].astype(str)
            
            return df
        except Exception as e:
            st.error(f"Error fetching NAV data for {category}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def calculate_rolling_returns(self, nav_df, rolling_period, return_type='median'):
        """
        Calculate rolling returns for each fund with different return types
        """
        # Check if nav_df is valid
        if nav_df is None or nav_df.empty:
            st.error("No NAV data available for calculating rolling returns.")
            return pd.DataFrame()

        # Remove any rows with NaN values
        nav_df = nav_df.dropna(subset=['net_asset_value'])

        try:
            # Create a unique dataframe of scheme codes and names
            scheme_info = nav_df[['scheme_code', 'scheme_name']].drop_duplicates()
            scheme_info['scheme_code'] = scheme_info['scheme_code'].astype(str)

            # Group by scheme code and calculate rolling returns
            def calculate_fund_rolling_returns(group):
                try:
                    # Sort by date
                    group = group.sort_values('date')
                    
                    # Calculate rolling returns
                    window = min(rolling_period, len(group))
                    returns = group['net_asset_value'].rolling(window=window).apply(
                        lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100 if len(x) > 1 else np.nan
                    )
                    
                    # Calculate return based on type
                    if return_type == 'median':
                        return_value = returns.median()
                    elif return_type == 'average':
                        return_value = returns.mean()
                    elif return_type == 'max':
                        return_value = returns.max()
                    elif return_type == 'min':
                        return_value = returns.min()
                    else:
                        return_value = returns.median()
                    
                    # Create a dataframe with the return
                    result = pd.DataFrame({
                        'date': [group['date'].iloc[-1]],
                        'rolling_return': [return_value],
                        'scheme_code': [group['scheme_code'].iloc[0]]
                    })
                    
                    return result
                except Exception as e:
                    st.warning(f"Error calculating returns for a fund: {e}")
                    return pd.DataFrame()

            # Apply rolling returns calculation
            rolling_returns = nav_df.groupby('scheme_code', group_keys=False).apply(calculate_fund_rolling_returns)
            
            # Merge with scheme info to get scheme names
            rolling_returns['scheme_code'] = rolling_returns['scheme_code'].astype(str)
            result = rolling_returns.merge(scheme_info, on='scheme_code', how='left')
            
            # Remove any rows with NaN
            result = result.dropna()
            
            return result

        except Exception as e:
            st.error(f"Error in calculating rolling returns: {e}")
            return pd.DataFrame()

    def calculate_maximum_drawdown(self, nav_df, period):
        """
        Calculate maximum drawdown percentage for each fund
        """
        # Check if nav_df is valid
        if nav_df is None or nav_df.empty:
            st.error("No NAV data available for calculating maximum drawdown.")
            return pd.DataFrame()

        # Remove any rows with NaN values
        nav_df = nav_df.dropna(subset=['net_asset_value'])

        def max_drawdown(series):
            try:
                cumulative = (series / series.iloc[0] - 1) * 100
                max_drop = (cumulative.cummax() - cumulative).max()
                return max_drop
            except Exception as e:
                st.warning(f"Could not calculate max drawdown for a fund: {e}")
                return np.nan

        try:
            # Create a unique dataframe of scheme codes and names
            scheme_info = nav_df[['scheme_code', 'scheme_name']].drop_duplicates()
            scheme_info['scheme_code'] = scheme_info['scheme_code'].astype(str)

            # Group by scheme code and calculate max drawdown
            max_drawdowns = nav_df.groupby('scheme_code').apply(
                lambda x: max_drawdown(x.set_index('date')['net_asset_value'])
            ).reset_index()
            
            # Rename columns
            max_drawdowns.columns = ['scheme_code', 'max_drawdown']
            
            # Convert scheme_code to string
            max_drawdowns['scheme_code'] = max_drawdowns['scheme_code'].astype(str)
            
            # Merge with scheme info
            result = max_drawdowns.merge(scheme_info, on='scheme_code', how='left')
            
            # Remove any rows with NaN
            result = result.dropna()
            
            return result

        except Exception as e:
            st.error(f"Error in calculating maximum drawdown: {e}")
            return pd.DataFrame()

def main():
    st.set_page_config(page_title="Mutual Fund Performance Heatmap", page_icon="📊", layout="wide")
    analyzer = MutualFundPerformanceAnalyzer()
    st.title("📈 Mutual Fund Performance Heatmap")

    # Columns for input selection
    col1, col2, col3, col4 = st.columns(4)

    # Select fund category
    with col1:
        fund_categories = analyzer.get_fund_categories()
        selected_category = st.selectbox("Select Fund Category", ['Select Category'] + fund_categories)

    # Select rolling returns period
    with col2:
        period_options = [30, 60, 90, 180, 365]
        selected_period = st.selectbox("Select Rolling Returns Period (Days)", period_options)

    # Select rolling returns type
    with col3:
        return_type_options = ['median', 'average', 'max', 'min']
        selected_return_type = st.selectbox("Select Rolling Returns Type", return_type_options)

    # Date range selection
    with col4:
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
        rolling_returns = analyzer.calculate_rolling_returns(nav_data, selected_period, selected_return_type)
        max_drawdowns = analyzer.calculate_maximum_drawdown(nav_data, (end_date - start_date).days)

        # Check if calculations were successful
        if rolling_returns.empty or max_drawdowns.empty:
            st.error("Could not calculate performance metrics.")
            return

        # Merge rolling returns and max drawdowns
        performance_df = rolling_returns.merge(
            max_drawdowns[['scheme_code', 'max_drawdown']], 
            on='scheme_code', 
            how='inner'
        )
        
        # Remove any rows with NaN
        performance_df = performance_df.dropna()

        if performance_df.empty:
            st.error("Could not merge performance metrics.")
            return

        # Create scatter plot with hover text
        fig = go.Figure(data=go.Scatter(
            x=performance_df['rolling_return'],
            y=performance_df['max_drawdown'],
            mode='markers',
            marker=dict(
                size=10,
                color=performance_df['rolling_return'],  # color by rolling returns
                colorscale='Viridis',
                colorbar=dict(title=f'{selected_return_type.capitalize()} Rolling Returns %'),
                showscale=True
            ),
            hovertemplate='<b>Fund Name</b>: %{text}<br>' + 
                          f'<b>{selected_return_type.capitalize()} Rolling Returns</b>: %{{x:.2f}}%<br>' + 
                          '<b>Max Drawdown</b>: %{y:.2f}%<extra></extra>',
            text=performance_df['scheme_name']
        ))

        # Update layout
        fig.update_layout(
            title=f'Fund Performance: {selected_return_type.capitalize()} Rolling Returns vs Max Drawdown ({selected_category})',
            xaxis_title=f'{selected_return_type.capitalize()} Rolling Returns % ({selected_period} Days)',
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
