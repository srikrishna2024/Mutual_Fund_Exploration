import streamlit as st
import pandas as pd
import psycopg
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
from plotly.subplots import make_subplots

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

def calculate_category_average(summary_data, period):
    """Safely calculate category average for a given period"""
    try:
        values = []
        for entry in summary_data:
            if period in entry and entry[period] is not None:
                try:
                    value = float(entry[period].replace('%', '').strip())
                    values.append(value)
                except (ValueError, AttributeError):
                    continue
        return f"{np.mean(values):.2f}%" if values else "N/A"
    except:
        return "N/A"

def main():
    """
    Main function to run the Streamlit app for Category-wide Mutual Fund Analysis.
    This function sets up the Streamlit page configuration, displays the title, and creates a sidebar for user inputs.
    It allows users to select a mutual fund category and analyze the rolling returns and risk for funds within that category.
    The analysis includes:
    - Returns Summary
    - Risk Summary
    - Individual Fund Analysis
    - Detailed Statistics
    The function fetches and processes NAV data for the selected category and displays the results in various tabs.
    Sidebar Inputs:
    - Mutual Fund Category: Dropdown to select the mutual fund category.
    - Analyze Category: Button to trigger the analysis.
    Tabs:
    - Returns Summary: Displays the returns summary for the selected category.
    - Risk Summary: Displays the risk summary (standard deviation) for the selected category.
    - Individual Fund Analysis: Allows detailed analysis of individual funds within the selected category.
    - Detailed Statistics: Provides detailed statistics for all funds in the selected category, with an option to download the data as a CSV file.
    """
    st.set_page_config(page_title='Category-wide Mutual Fund Analysis', layout='wide')
    st.title('Mutual Fund Category Analysis - Rolling Returns')

    # Sidebar for inputs
    st.sidebar.header('Select Parameters')
    
    # Get categories
    categories = get_categories()
    selected_category = st.sidebar.selectbox('Select Mutual Fund Category', categories)

    # Define time periods
    periods = {
        '1 Month': 30,
        '3 Months': 90,
        '6 Months': 180,
        '1 Year': 365,
        '3 Years': 1095,
        '5 Years': 1825,
        '10 Years': 3650
    }

    analyze_button = st.sidebar.button('Analyze Category')

    if analyze_button:
        schemes = get_schemes_by_category(selected_category)
        
        if not schemes:
            st.error('No funds found in the selected category.')
            return

        st.info(f'Analyzing {len(schemes)} funds in {selected_category}')
        
        tab1, tab2, tab3, tab4 = st.tabs(['Returns Summary', 'Risk Summary', 'Individual Fund Analysis', 'Detailed Statistics'])

        # Store all fund data
        all_fund_data = {}
        returns_summary = []
        risk_summary = []

        with st.spinner('Fetching and analyzing data for all funds...'):
            for scheme_name, scheme_code in schemes.items():
                nav_data = get_nav_data(scheme_code)
                if not nav_data.empty:
                    fund_data = {}
                    returns_data = {'Fund Name': scheme_name}
                    risk_data = {'Fund Name': scheme_name}
                    
                    for period_name in periods.keys():
                        window_days = periods[period_name]
                        rolling_returns = calculate_rolling_returns(nav_data, window_days)
                        if not rolling_returns.empty:
                            fund_data[period_name] = rolling_returns
                            returns_data[period_name] = f"{(rolling_returns.mean() * 100):.2f}%"
                            risk_data[period_name] = f"{(rolling_returns.std() * 100):.2f}%"
                        else:
                            returns_data[period_name] = "N/A"
                            risk_data[period_name] = "N/A"
                    
                    if fund_data:
                        all_fund_data[scheme_name] = fund_data
                        returns_summary.append(returns_data)
                        risk_summary.append(risk_data)

            # Display Returns Summary
            with tab1:
                st.subheader('Category Returns Summary (%)')
                if returns_summary:
                    returns_df = pd.DataFrame(returns_summary)
                    st.dataframe(returns_df.set_index('Fund Name'), use_container_width=True)
                    
                    # Calculate and display average returns
                    returns_data = {}
                    for period in periods.keys():
                        returns_data[period] = calculate_category_average(returns_summary, period)
                    
                    st.subheader('Category Average Returns')
                    avg_returns_df = pd.DataFrame([returns_data])
                    st.dataframe(avg_returns_df, use_container_width=True)

            # Display Risk Summary
            with tab2:
                st.subheader('Category Risk Summary (Standard Deviation %)')
                if risk_summary:
                    risk_df = pd.DataFrame(risk_summary)
                    st.dataframe(risk_df.set_index('Fund Name'), use_container_width=True)
                    
                    # Calculate and display average risk
                    risk_data = {}
                    for period in periods.keys():
                        risk_data[period] = calculate_category_average(risk_summary, period)
                    
                    st.subheader('Category Average Risk')
                    avg_risk_df = pd.DataFrame([risk_data])
                    st.dataframe(avg_risk_df, use_container_width=True)

            # Individual Fund Analysis
            with tab3:
                st.subheader('Individual Fund Analysis')
                selected_fund = st.selectbox('Select Fund for Detailed Analysis', 
                                           list(all_fund_data.keys()))
                
                if selected_fund in all_fund_data:
                    fund_data = all_fund_data[selected_fund]
                    
                    num_periods = len(periods)
                    fig = make_subplots(rows=num_periods, cols=1, 
                                      subplot_titles=list(periods.keys()),
                                      vertical_spacing=0.05)
                    
                    for i, period_name in enumerate(periods.keys(), 1):
                        if period_name in fund_data:
                            returns = fund_data[period_name]
                            fig.add_trace(
                                go.Scatter(x=returns.index, 
                                         y=returns * 100,
                                         name=period_name,
                                         showlegend=False),
                                row=i, col=1
                            )
                            fig.add_hline(y=0, line_dash="dash", line_color="gray", 
                                        opacity=0.5, row=i, col=1)
                    
                    fig.update_layout(height=200 * num_periods,
                                    title_text=f"Rolling Returns Analysis - {selected_fund}",
                                    showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

            # Detailed Statistics
            with tab4:
                st.subheader('Detailed Statistics')
                detailed_stats = []
                for fund_name, fund_data in all_fund_data.items():
                    fund_stats = {'Fund Name': fund_name}
                    for period_name in periods.keys():
                        if period_name in fund_data:
                            returns = fund_data[period_name]
                            fund_stats.update({
                                f'{period_name} Avg Return (%)': f"{(returns.mean() * 100):.2f}",
                                f'{period_name} Std Dev (%)': f"{(returns.std() * 100):.2f}",
                                f'{period_name} Max Return (%)': f"{(returns.max() * 100):.2f}",
                                f'{period_name} Min Return (%)': f"{(returns.min() * 100):.2f}"
                            })
                        else:
                            fund_stats.update({
                                f'{period_name} Avg Return (%)': "N/A",
                                f'{period_name} Std Dev (%)': "N/A",
                                f'{period_name} Max Return (%)': "N/A",
                                f'{period_name} Min Return (%)': "N/A"
                            })
                    detailed_stats.append(fund_stats)
                
                if detailed_stats:
                    detailed_df = pd.DataFrame(detailed_stats)
                    st.dataframe(detailed_df.set_index('Fund Name'), use_container_width=True)
                    
                    # Download button for detailed statistics
                    csv = detailed_df.to_csv(index=False)
                    st.download_button(
                        label="Download Detailed Statistics as CSV",
                        data=csv,
                        file_name=f"mutual_fund_detailed_stats_{selected_category}.csv",
                        mime="text/csv"
                    )

if __name__ == "__main__":
    main()