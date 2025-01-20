import streamlit as st
import pandas as pd
import numpy as np
import psycopg
import plotly.express as px
import plotly.graph_objects as go

def connect_to_db():
    """Create database connection"""
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }
    return psycopg.connect(**DB_PARAMS)

def get_portfolio_data():
    """Retrieve portfolio data"""
    with connect_to_db() as conn:
        query = """
            SELECT date, scheme_name, code, transaction_type, value, units, amount 
            FROM portfolio_data 
            ORDER BY date, scheme_name
        """
        return pd.read_sql(query, conn)

def get_historical_nav(portfolio_funds):
    """Retrieve historical NAV data"""
    with connect_to_db() as conn:
        query = """
            SELECT code, scheme_name, nav as date, value as nav_value
            FROM mutual_fund_nav
            WHERE code = ANY(%s)
            ORDER BY code, nav
        """
        return pd.read_sql(query, conn, params=(portfolio_funds,))

def calculate_fund_metrics(df, historical_nav):
    """Calculate volatility metrics for each fund"""
    portfolio_funds = df.groupby('code')['units'].sum()
    portfolio_funds = portfolio_funds[portfolio_funds > 0].index.tolist()

    # Calculate returns
    nav_data = historical_nav[historical_nav['code'].isin(portfolio_funds)]
    nav_pivot = nav_data.pivot(index='date', columns='code', values='nav_value')
    daily_returns = nav_pivot.pct_change()

    # Calculate weights
    current_nav = nav_data.groupby('code')['nav_value'].last()
    current_units = df.groupby('code')['units'].sum()
    fund_values = current_nav * current_units
    total_value = fund_values.sum()
    weights = fund_values / total_value

    # Calculate volatility metrics
    volatility_metrics = pd.DataFrame()
    volatility_metrics['Daily Volatility'] = daily_returns.std()
    volatility_metrics['Annualized Volatility'] = volatility_metrics['Daily Volatility'] * np.sqrt(252)
    volatility_metrics['Weight'] = weights

    # Calculate covariance and correlation
    daily_covariance = daily_returns.cov()
    correlation_matrix = daily_returns.corr()

    # Calculate Marginal Contribution to Risk (MCR)
    portfolio_variance = np.dot(weights, np.dot(daily_covariance, weights))
    portfolio_volatility = np.sqrt(portfolio_variance)

    mcr = np.dot(daily_covariance, weights) / portfolio_volatility
    volatility_metrics['MCR'] = mcr

    # Total Contribution to Risk (TCR)
    tcr = weights * mcr
    volatility_metrics['TCR'] = tcr

    # Risk contribution percentage
    volatility_metrics['Risk Contribution %'] = (tcr / portfolio_volatility) * 100

    return volatility_metrics, correlation_matrix, portfolio_volatility * np.sqrt(252)

def analyze_risk_factors(volatility_metrics):
    """Analyze why a fund has high risk contribution"""
    fund_analysis = {}
    for fund in volatility_metrics.index:
        weight = volatility_metrics.loc[fund, 'Weight']
        vol = volatility_metrics.loc[fund, 'Annualized Volatility']
        risk_contrib = volatility_metrics.loc[fund, 'Risk Contribution %']

        # Compare against averages
        avg_weight = volatility_metrics['Weight'].mean()
        avg_vol = volatility_metrics['Annualized Volatility'].mean()

        weight_factor = weight / avg_weight
        vol_factor = vol / avg_vol

        # Determine primary risk factor
        if weight_factor > vol_factor:
            primary_factor = "weight"
            factor_ratio = weight_factor
        else:
            primary_factor = "volatility"
            factor_ratio = vol_factor

        fund_analysis[fund] = {
            'primary_factor': primary_factor,
            'factor_ratio': factor_ratio,
            'weight_vs_avg': weight_factor,
            'vol_vs_avg': vol_factor
        }

    return fund_analysis

def main():
    """
    Main function to run the Portfolio Volatility Analysis application.
    This function sets up the Streamlit page configuration, loads portfolio data,
    calculates volatility metrics, and displays various analyses and visualizations
    related to portfolio risk and fund performance.
    The function performs the following steps:
    1. Sets the page title and layout.
    2. Loads portfolio data and checks for its availability.
    3. Loads historical NAV data for the portfolio funds.
    4. Calculates volatility metrics, correlation matrix, and portfolio volatility.
    5. Displays portfolio risk overview including annualized volatility and highest risk contribution.
    6. Displays individual fund analysis with metrics and primary risk factors.
    7. Visualizes risk contribution analysis using bar charts.
    8. Displays fund correlation analysis using a heatmap.
    9. Handles exceptions and displays error messages if any issues occur.
    Raises:
        Exception: If there is an error in loading data, calculating metrics, or any other step.
    """
    st.set_page_config(page_title="Portfolio Volatility Analysis", layout="wide")
    st.title("Fund Volatility and Risk Analysis Dashboard")

    try:
        # Load data
        df = get_portfolio_data()
        if df.empty:
            st.warning("No portfolio data found.")
            return

        portfolio_funds = df.groupby('code')['units'].sum()
        portfolio_funds = portfolio_funds[portfolio_funds > 0].index.tolist()

        historical_nav = get_historical_nav(portfolio_funds)
        if historical_nav.empty:
            st.warning("No NAV data found.")
            return

        # Calculate metrics
        volatility_metrics, correlation_matrix, portfolio_volatility = calculate_fund_metrics(df, historical_nav)

        # Display Portfolio Overview
        st.header("Portfolio Risk Overview")
        col1, col2 = st.columns(2)

        with col1:
            st.metric("Portfolio Annualized Volatility", f"{portfolio_volatility:.2f}%")

        with col2:
            highest_risk_fund = volatility_metrics['Risk Contribution %'].idxmax()
            st.metric(
                "Highest Risk Contribution", 
                f"{volatility_metrics.loc[highest_risk_fund, 'Risk Contribution %']:.2f}%", 
                f"from {historical_nav[historical_nav['code'] == highest_risk_fund]['scheme_name'].iloc[0]}"
            )

        # Individual Fund Analysis
        st.header("Individual Fund Analysis")

        fund_metrics = volatility_metrics.copy()
        fund_metrics.index = [historical_nav[historical_nav['code'] == code]['scheme_name'].iloc[0] 
                            for code in fund_metrics.index]

        # Add Primary Risk Factor Analysis
        risk_factors = analyze_risk_factors(volatility_metrics)
        fund_metrics['Primary Risk Factor'] = [risk_factors[code]['primary_factor'] for code in volatility_metrics.index]

        # Format metrics for display
        display_metrics = fund_metrics.copy()
        for col in ['Daily Volatility', 'Annualized Volatility', 'MCR', 'TCR', 'Risk Contribution %']:
            display_metrics[col] = display_metrics[col].map('{:.2f}%'.format)
        display_metrics['Weight'] = display_metrics['Weight'].map('{:.2f}%'.format)

        st.dataframe(display_metrics)

        st.info("**How to Interpret the Results:**\n- **Weight:** Proportion of portfolio value held in the fund.\n- **Annualized Volatility:** Risk level of the fund.\n- **Risk Contribution %:** Indicates the fund's contribution to overall portfolio risk.\n- **Primary Risk Factor:** Highlights whether the fund's weight or volatility drives its risk contribution.")

        # Risk Contribution Visualization
        st.header("Risk Contribution Analysis")

        fig = go.Figure(data=[
            go.Bar(name='Weight', 
                  x=fund_metrics.index, 
                  y=fund_metrics['Weight'] * 100),
            go.Bar(name='Risk Contribution', 
                  x=fund_metrics.index, 
                  y=fund_metrics['Risk Contribution %'])
        ])

        fig.update_layout(
            barmode='group',
            title="Fund Weights vs Risk Contribution",
            xaxis_title="Fund Name",
            yaxis_title="Percentage (%)",
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

        # Correlation Analysis
        st.header("Fund Correlation Analysis")

        correlation_display = correlation_matrix.copy()
        correlation_display.index = [historical_nav[historical_nav['code'] == code]['scheme_name'].iloc[0] 
                                   for code in correlation_display.index]
        correlation_display.columns = correlation_display.index

        fig = px.imshow(correlation_display,
                       labels=dict(x="Fund", y="Fund", color="Correlation"),
                       color_continuous_scale="RdBu_r",
                       aspect="auto")

        fig.update_layout(
            title="Fund Correlation Heatmap",
            height=600
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.error("Please check your database connection and data integrity.")

if __name__ == "__main__":
    main()
