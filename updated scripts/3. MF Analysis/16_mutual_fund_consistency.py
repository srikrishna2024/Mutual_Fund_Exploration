import streamlit as st
import pandas as pd
import psycopg
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

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
    """Fetch unique scheme categories"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT scheme_category 
                FROM mutual_fund_master_data 
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
                ORDER BY scheme_name;
            """, (category,))
            return {row[0]: row[1] for row in cur.fetchall()}


def get_nav_data(scheme_code):
    """Fetch NAV data for selected scheme"""
    with connect_to_db() as conn:
        query = """
            SELECT nav::date AS date, value::float AS nav 
            FROM mutual_fund_nav 
            WHERE code = %s 
            AND value > 0
            ORDER BY nav;
        """
        df = pd.read_sql(query, conn, params=(scheme_code,))
        df['date'] = pd.to_datetime(df['date'])
        return df

def calculate_rolling_returns(nav_data, window_days):
    """Calculate rolling returns for given window period"""
    nav_data = nav_data.set_index('date').sort_index()
    returns = nav_data['nav'].pct_change(periods=window_days)
    rolling_returns = (1 + returns) ** (365 / window_days) - 1
    return rolling_returns.dropna()

def calculate_risk_metrics(nav_data, rolling_periods):
    """Calculate risk metrics for all rolling periods"""
    metrics = []

    for period_name, window_days in rolling_periods.items():
        rolling_returns = calculate_rolling_returns(nav_data, window_days)

        if not rolling_returns.empty:
            # Standard deviation
            std_dev = rolling_returns.std() * 100

            # Sharpe ratio (assuming risk-free rate of 4%)
            risk_free_rate = 0.04
            excess_returns = rolling_returns - (risk_free_rate / 365)
            sharpe_ratio = excess_returns.mean() / rolling_returns.std()

            # Upside and downside ratios
            upside_returns = rolling_returns[rolling_returns > 0]
            downside_returns = rolling_returns[rolling_returns < 0]

            upside_ratio = upside_returns.mean() / rolling_returns.std() if not upside_returns.empty else 0
            downside_ratio = abs(downside_returns.mean() / rolling_returns.std()) if not downside_returns.empty else 0

            # Consistency Score (Quartile Ranking)
            consistency_score = calculate_consistency_score(rolling_returns)

            # Add metrics for this period
            metrics.append({
                'Period': period_name,
                'Std Dev (%)': f'{std_dev:.2f}',
                'Sharpe Ratio': f'{sharpe_ratio:.2f}',
                'Upside Ratio': f'{upside_ratio:.2f}',
                'Downside Ratio': f'{downside_ratio:.2f}',
                'Consistency Score': consistency_score  # Leave as numeric
            })

    return pd.DataFrame(metrics)

def calculate_consistency_score(rolling_returns):
    """Calculate Consistency Score (Quartile Ranking)"""
    quartiles = pd.qcut(rolling_returns.rank(method='first'), 4, labels=False)
    return quartiles.mean()

def main():
    Main function to run the Mutual Fund Analysis Streamlit application.
    This function sets up the Streamlit page configuration, creates tabs for different analyses,
    and handles user interactions for analyzing mutual fund categories and specific funds.
    Tabs:
    - Category Risk Metrics: Allows users to select a scheme category and filter periods to analyze
      risk metrics for all funds in the selected category. Displays combined metrics and insights,
      and identifies the top and bottom consistent funds based on Upside and Downside Ratios.
    - Fund Analysis: Allows users to select a scheme category and a specific fund to analyze.
      Displays rolling returns and risk metrics for the selected fund.
    User Interactions:
    - Select scheme category and filter periods for category risk metrics analysis.
    - Select scheme category and specific fund for individual fund analysis.
    - Analyze buttons to trigger data fetching and analysis.
    Data Fetching and Analysis:
    - Fetches categories and schemes by category.
    - Fetches NAV data for selected schemes.
    - Calculates rolling returns and risk metrics for selected periods.
    - Displays results in tables, dataframes, and plots.
    Note:
    - Requires external functions: get_categories, get_schemes_by_category, get_nav_data,
      calculate_risk_metrics, calculate_rolling_returns.
    - Uses Streamlit for UI components and Plotly for plotting.
    st.set_page_config(page_title='Mutual Fund Analysis', layout='wide')
    st.title('Mutual Fund Analysis')

    # Fetch categories
    categories = get_categories()

    # Create tabs
    tab1, tab2 = st.tabs(['Category Risk Metrics', 'Fund Analysis'])

    # Tab 1: Display risk metrics for all funds in a category
    with tab1:
        st.subheader('Category Risk Metrics')
        selected_category = st.selectbox('Select Scheme Category', categories, key='tab1_category')

        filter_period = st.multiselect('Filter by Period', ['3 Months', '6 Months', '1 Year', '3 Years', '5 Years', '10 Years'], default=['3 Months', '6 Months', '1 Year'], key='tab1_period')

        analyze_button = st.button('Analyze Category', key='analyze_category_button')

        if selected_category and analyze_button:
            with st.spinner('Fetching data for all funds...'):
                schemes = get_schemes_by_category(selected_category)
                all_risk_metrics = []

                for scheme_name, scheme_code in schemes.items():
                    nav_data = get_nav_data(scheme_code)

                    if not nav_data.empty:
                        rolling_periods = {
                            '3 Months': 90,
                            '6 Months': 180,
                            '1 Year': 365,
                            '3 Years': 1095,
                            '5 Years': 1825,
                            '10 Years': 3650
                        }
                        filtered_periods = {k: rolling_periods[k] for k in filter_period}

                        risk_metrics = calculate_risk_metrics(nav_data, filtered_periods)
                        risk_metrics['Fund'] = scheme_name
                        all_risk_metrics.append(risk_metrics)

                if all_risk_metrics:
                    combined_metrics = pd.concat(all_risk_metrics, ignore_index=True)

                    # Convert necessary columns to numeric
                    combined_metrics['Upside Ratio'] = pd.to_numeric(combined_metrics['Upside Ratio'], errors='coerce')
                    combined_metrics['Downside Ratio'] = pd.to_numeric(combined_metrics['Downside Ratio'], errors='coerce')

                    # Display combined metrics
                    st.dataframe(combined_metrics)

                    # Display insights
                    st.subheader('Insights')
                    st.markdown(
                        """Interpretation of Results:
                        - **Std Dev (%)**: Measures volatility; lower values indicate more stability.
                        - **Sharpe Ratio**: Higher values imply better risk-adjusted returns.
                        - **Upside/Downside Ratios**: Higher upside and lower downside ratios are preferred.
                        - **Consistency Score**: Indicates how consistently a fund performs well over the period. Higher scores are better.
                        """
                    )

                    # Find top and bottom consistent funds based on Upside and Downside Ratios
                    avg_ratios = combined_metrics.groupby('Fund').agg({
                        'Upside Ratio': 'mean',
                        'Downside Ratio': 'mean'
                    })
                    avg_ratios['Score'] = avg_ratios['Upside Ratio'] - avg_ratios['Downside Ratio']
                    top_5_funds = avg_ratios.sort_values(by='Score', ascending=False).head(5).index
                    bottom_5_funds = avg_ratios.sort_values(by='Score', ascending=False).tail(5).index

                    st.subheader('Top 5 Most Consistent Funds')
                    st.write(combined_metrics[combined_metrics['Fund'].isin(top_5_funds)])

                    st.subheader('Top 5 Least Consistent Funds')
                    st.write(combined_metrics[combined_metrics['Fund'].isin(bottom_5_funds)])
                else:
                    st.warning('No data available for the selected category.')

    # Tab 2: Analyze specific fund
    with tab2:
        st.subheader('Fund Analysis')
        selected_category = st.selectbox('Select Scheme Category', categories, key='tab2_category')

        if selected_category:
            schemes = get_schemes_by_category(selected_category)
            selected_scheme = st.selectbox('Select Fund', list(schemes.keys()), key='tab2_fund')

            if selected_scheme:
                analyze_button = st.button('Analyze', key='analyze_button')

                if analyze_button:
                    scheme_code = schemes[selected_scheme]

                    with st.spinner('Fetching and analyzing data...'):
                        nav_data = get_nav_data(scheme_code)

                        if nav_data.empty:
                            st.warning('No data available for the selected fund.')
                            return

                        # Define rolling periods
                        rolling_periods = {
                            '3 Months': 90,
                            '6 Months': 180,
                            '1 Year': 365,
                            '3 Years': 1095,
                            '5 Years': 1825,
                            '10 Years': 3650
                        }

                        # Plot rolling returns
                        for period_name, window_days in rolling_periods.items():
                            rolling_returns = calculate_rolling_returns(nav_data, window_days)

                            if not rolling_returns.empty:
                                fig = go.Figure()
                                fig.add_trace(go.Scatter(
                                    x=rolling_returns.index,
                                    y=rolling_returns * 100,
                                    mode='lines',
                                    name=f'{period_name} Rolling Return'
                                ))

                                fig.update_layout(
                                    title=f'{period_name} Rolling Returns for {selected_scheme}',
                                    xaxis_title='Date',
                                    yaxis_title='Annualized Rolling Return (%)',
                                    height=400
                                )

                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning(f'Insufficient data to calculate {period_name} rolling returns.')

                        # Calculate and display risk metrics
                        st.subheader('Risk Metrics')
                        risk_metrics = calculate_risk_metrics(nav_data, rolling_periods)
                        st.table(risk_metrics)

if __name__ == "__main__":
    main()
