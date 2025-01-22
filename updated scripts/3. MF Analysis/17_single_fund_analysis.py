import streamlit as st
import pandas as pd
import psycopg
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from scipy import stats

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

def connect_to_db():
    """Create database connection"""
    return psycopg.connect(**DB_PARAMS)

def get_categories():
    """Fetch unique scheme categories for open ended funds"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT scheme_category 
                FROM mutual_fund_master_data 
                WHERE scheme_type = 'Open Ended'
                ORDER BY scheme_category;
            """)
            return [row[0] for row in cur.fetchall()]

def get_schemes_by_category(category):
    """Fetch schemes for selected category"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT scheme_name, code 
                FROM mutual_fund_master_data 
                WHERE scheme_category = %s 
                AND scheme_type = 'Open Ended'
                ORDER BY scheme_name;
            """, (category,))
            return {row[0]: row[1] for row in cur.fetchall()}

def get_nav_data(scheme_code, start_date=None):
    """Fetch NAV data for selected scheme"""
    with connect_to_db() as conn:
        query = """
            SELECT nav::date as date, value::float as nav
            FROM mutual_fund_nav 
            WHERE code = %s 
            AND nav >= COALESCE(%s::date, '1900-01-01'::date)
            AND value > 0
            ORDER BY nav;
        """
        df = pd.read_sql(query, conn, params=(scheme_code, start_date))
        df['date'] = pd.to_datetime(df['date'])
        return df

def calculate_returns(prices, window_days):
    """Calculate returns for given window period"""
    if window_days >= len(prices):
        return pd.Series(index=prices.index)
        
    # Calculate rolling returns using the correct formula for annualized returns
    rolling_returns = (prices / prices.shift(window_days)) ** (365/window_days) - 1
    return (rolling_returns * 100).round(2)

def calculate_risk_metrics(nav_data):
    """
    Calculate various risk metrics for the mutual fund:
    - Annualized Return
    - Annualized Volatility
    - Sharpe Ratio (assuming risk-free rate of 4%)
    - Maximum Drawdown
    - Sortino Ratio
    - Value at Risk (VaR)
    """
    try:
        if nav_data.empty or len(nav_data) < 30:  # Need at least 30 days of data
            return None
            
        # Calculate daily returns
        df = nav_data.copy()
        df['daily_returns'] = df['nav'].pct_change()
        df = df.dropna()
        
        if df.empty:
            return None
            
        # Risk-free rate (assuming 4% annual)
        risk_free_rate = 0.04
        
        # Calculate metrics
        metrics = []
        
        # 1. Annualized Return
        total_days = (df.index[-1] - df.index[0]).days
        total_return = (df['nav'].iloc[-1] / df['nav'].iloc[0]) - 1
        ann_return = (1 + total_return) ** (365/total_days) - 1
        metrics.append({
            'Metric': 'Annualized Return',
            'Value': f'{(ann_return * 100):.2f}%'
        })
        
        # 2. Annualized Volatility
        ann_vol = df['daily_returns'].std() * np.sqrt(252)
        metrics.append({
            'Metric': 'Annualized Volatility',
            'Value': f'{(ann_vol * 100):.2f}%'
        })
        
        # 3. Sharpe Ratio
        excess_returns = ann_return - risk_free_rate
        sharpe_ratio = excess_returns / ann_vol if ann_vol != 0 else 0
        metrics.append({
            'Metric': 'Sharpe Ratio',
            'Value': f'{sharpe_ratio:.2f}'
        })
        
        # 4. Maximum Drawdown
        cumulative_returns = (1 + df['daily_returns']).cumprod()
        rolling_max = cumulative_returns.expanding().max()
        drawdowns = cumulative_returns / rolling_max - 1
        max_drawdown = drawdowns.min()
        metrics.append({
            'Metric': 'Maximum Drawdown',
            'Value': f'{(max_drawdown * 100):.2f}%'
        })
        
        # 5. Sortino Ratio
        negative_returns = df['daily_returns'][df['daily_returns'] < 0]
        downside_std = negative_returns.std() * np.sqrt(252)
        sortino_ratio = excess_returns / downside_std if downside_std != 0 else 0
        metrics.append({
            'Metric': 'Sortino Ratio',
            'Value': f'{sortino_ratio:.2f}'
        })
        
        # 6. Value at Risk (95% confidence)
        var_95 = np.percentile(df['daily_returns'], 5)
        metrics.append({
            'Metric': 'Daily VaR (95%)',
            'Value': f'{(abs(var_95) * 100):.2f}%'
        })
        
        return pd.DataFrame(metrics)
        
    except Exception as e:
        st.error(f"Error calculating risk metrics: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def calculate_rolling_returns(nav_data, window_days=365):
    """Calculate rolling returns for given window period"""
    try:
        if nav_data.empty or len(nav_data) < window_days:
            return None
        
        df = nav_data.copy()
        df = df.set_index('date')
        df = df.sort_index()  # Ensure data is sorted by date
        
        # Calculate rolling returns
        rolling_returns = calculate_returns(df['nav'], window_days)
        
        # Create result DataFrame and handle NaN values
        result = pd.DataFrame({
            'Date': rolling_returns.index,
            'Rolling Returns (%)': rolling_returns.values
        }).dropna()
        
        return result if not result.empty else None
        
    except Exception as e:
        st.error(f"Error calculating rolling returns: {str(e)}")
        return None

def single_fund_analysis():
    st.subheader('Single Fund Analysis')
    
    col1, col2 = st.columns(2)
    
    with col1:
        categories = get_categories()
        selected_category = st.selectbox('Select Scheme Category', categories)
    
    with col2:
        schemes = get_schemes_by_category(selected_category)
        selected_scheme = st.selectbox('Select Scheme', list(schemes.keys()))
    
    col3, col4 = st.columns(2)
    
    with col3:
        period_mapping = {
            'YTD': (datetime.now() - datetime.strptime(f"{datetime.now().year}-01-01", "%Y-%m-%d")).days,
            '1 Year': 365,
            '2 Years': 730,
            '3 Years': 1095,
            '5 Years': 1825,
            'Max': None
        }
        selected_period = st.selectbox('Select Analysis Period', list(period_mapping.keys()))
    
    with col4:
        st.write("")
        st.write("")
        analyze_button = st.button('Analyze Fund', use_container_width=True)
    
    if analyze_button and selected_scheme:
        try:
            scheme_code = schemes[selected_scheme]
            start_date = None
            if period_mapping[selected_period]:
                start_date = datetime.now().date() - timedelta(days=period_mapping[selected_period])
            
            with st.spinner('Fetching and analyzing data...'):
                nav_data = get_nav_data(scheme_code, start_date)
                
                if nav_data.empty:
                    st.warning('No data available for the selected period.')
                    return
                
                # Display data points for debugging
                st.write(f"Total data points: {len(nav_data)}")
                
                rolling_returns = calculate_rolling_returns(nav_data)
                if rolling_returns is not None and not rolling_returns.empty:
                    # Display rolling returns statistics for debugging
                    st.write(f"Rolling returns data points: {len(rolling_returns)}")
                    st.write(f"Date range: {rolling_returns['Date'].min()} to {rolling_returns['Date'].max()}")
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=rolling_returns['Date'],
                        y=rolling_returns['Rolling Returns (%)'],
                        name='Rolling Returns',
                        line=dict(color='#1f77b4'),
                        mode='lines'
                    ))
                    fig.update_layout(
                        title=f'Rolling Returns Analysis ({selected_period})',
                        xaxis_title='Date',
                        yaxis_title='Rolling Returns (%)',
                        hovermode='x unified',
                        showlegend=True,
                        height=600
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning('Insufficient data for rolling returns calculation.')
                
                risk_metrics = calculate_risk_metrics(nav_data)
                if risk_metrics is not None:
                    st.subheader('Risk Metrics')
                    st.table(risk_metrics.set_index('Metric'))
                
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.error(f"Error details: {type(e).__name__}")

def compare_funds():
    """
    Displays a Streamlit interface for comparing mutual funds based on selected criteria.
    The function allows users to:
    - Select a scheme category.
    - Select up to 3 schemes within the chosen category for comparison.
    - Choose an analysis period (e.g., YTD, 1 Year, 2 Years, etc.).
    - Compare the selected schemes based on rolling returns and risk metrics.
    The function fetches and processes the necessary data, and displays:
    - A plot of rolling returns for the selected schemes.
    - A table comparing risk metrics for the selected schemes.
    Raises:
        Exception: If an error occurs during data fetching or processing.
    Note:
        This function relies on several helper functions:
        - get_categories(): Fetches available scheme categories.
        - get_schemes_by_category(category): Fetches schemes for a given category.
        - get_nav_data(scheme_code, start_date): Fetches NAV data for a scheme.
        - calculate_rolling_returns(nav_data): Calculates rolling returns from NAV data.
        - calculate_risk_metrics(nav_data): Calculates risk metrics from NAV data.
    """
    st.subheader('Fund Comparison')
    
    col1, col2 = st.columns(2)
    
    with col1:
        categories = get_categories()
        selected_category = st.selectbox('Select Scheme Category', categories, key='compare_category')
    
    with col2:
        schemes = get_schemes_by_category(selected_category)
        selected_schemes = st.multiselect('Select up to 3 schemes to compare', 
                                        list(schemes.keys()),
                                        max_selections=3)
    
    col3, col4 = st.columns(2)
    
    with col3:
        period_mapping = {
            'YTD': (datetime.now() - datetime.strptime(f"{datetime.now().year}-01-01", "%Y-%m-%d")).days,
            '1 Year': 365,
            '2 Years': 730,
            '3 Years': 1095,
            '5 Years': 1825,
            'Max': None
        }
        selected_period = st.selectbox('Select Analysis Period', list(period_mapping.keys()), key='compare_period')
    
    with col4:
        st.write("")
        st.write("")
        compare_button = st.button('Compare Funds', use_container_width=True)
    
    if compare_button and selected_schemes:
        try:
            with st.spinner('Fetching and comparing data...'):
                start_date = None
                if period_mapping[selected_period]:
                    start_date = datetime.now().date() - timedelta(days=period_mapping[selected_period])
                
                fig = go.Figure()
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
                comparison_metrics = []
                
                for i, scheme_name in enumerate(selected_schemes):
                    scheme_code = schemes[scheme_name]
                    nav_data = get_nav_data(scheme_code, start_date)
                    
                    if nav_data.empty:
                        st.warning(f'No data available for {scheme_name}.')
                        continue
                    
                    rolling_returns = calculate_rolling_returns(nav_data)
                    if rolling_returns is not None and not rolling_returns.empty:
                        fig.add_trace(go.Scatter(
                            x=rolling_returns['Date'],
                            y=rolling_returns['Rolling Returns (%)'],
                            name=scheme_name,
                            line=dict(color=colors[i % len(colors)])
                        ))
                    
                    risk_metrics = calculate_risk_metrics(nav_data)
                    if risk_metrics is not None:
                        risk_metrics['Scheme'] = scheme_name
                        comparison_metrics.append(risk_metrics)
                
                if comparison_metrics:
                    fig.update_layout(
                        title=f'Rolling Returns Comparison ({selected_period})',
                        xaxis_title='Date',
                        yaxis_title='Rolling Returns (%)',
                        showlegend=True,
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.subheader('Risk Metrics Comparison')
                    comparison_df = pd.concat(comparison_metrics, axis=0)
                    comparison_pivot = comparison_df.pivot(columns='Scheme', values='Value', index='Metric')
                    st.table(comparison_pivot)
                else:
                    st.warning('No valid data available for comparison.')
        
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

def main():
    st.set_page_config(page_title='Mutual Fund Analysis', layout='wide')
    
    st.title('Mutual Fund Analysis Dashboard')
    
    tab1, tab2 = st.tabs(['Single Fund Analysis', 'Fund Comparison'])
    
    with tab1:
        single_fund_analysis()
    
    with tab2:
        compare_funds()

if __name__ == "__main__":
    main()