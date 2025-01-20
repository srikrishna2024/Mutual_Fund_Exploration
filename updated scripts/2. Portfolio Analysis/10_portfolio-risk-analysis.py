import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg
from scipy.optimize import newton

def format_indian_number(number):
    """
    Format a number in Indian style (lakhs, crores)
    """
    if number >= 10000000:  # crores
        return f"₹{number/10000000:.2f} Cr"
    elif number >= 100000:  # lakhs
        return f"₹{number/100000:.2f} L"
    elif number >= 1000:  # thousands
        return f"₹{number/1000:.2f} K"
    else:
        return f"₹{number:.2f}"

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
    """Retrieve all records from portfolio_data table"""
    with connect_to_db() as conn:
        query = """
            SELECT date, scheme_name, code, transaction_type, value, units, amount 
            FROM portfolio_data 
            ORDER BY date, scheme_name
        """
        return pd.read_sql(query, conn)

def get_portfolio_funds(df):
    """Get list of funds currently in portfolio"""
    fund_units = df.groupby('code')['units'].sum()
    return fund_units[fund_units > 0].index.tolist()

def get_latest_nav(portfolio_funds):
    """Retrieve the latest NAVs for portfolio funds"""
    with connect_to_db() as conn:
        query = """
            SELECT code, scheme_name, nav as date, value as nav_value
            FROM mutual_fund_nav
            WHERE (code, nav) IN (
                SELECT code, MAX(nav) AS latest_date
                FROM mutual_fund_nav
                WHERE code = ANY(%s)
                GROUP BY code
            )
        """
        return pd.read_sql(query, conn, params=(portfolio_funds,))

def get_historical_nav(portfolio_funds):
    """Retrieve historical NAV data for portfolio funds"""
    with connect_to_db() as conn:
        query = """
            SELECT code, scheme_name, nav as date, value as nav_value
            FROM mutual_fund_nav
            WHERE code = ANY(%s)
            ORDER BY code, nav
        """
        return pd.read_sql(query, conn, params=(portfolio_funds,))

def prepare_cashflows(df):
    """Prepare cashflow data from portfolio transactions"""
    df['cashflow'] = df.apply(lambda x: 
        -x['amount'] if x['transaction_type'] == 'invest'
        else x['amount'] if x['transaction_type'] == 'redeem'
        else (-x['amount'] if x['transaction_type'] == 'switch_out' else x['amount']), 
        axis=1
    )
    return df

def xirr(transactions):
    """Calculate XIRR given a set of transactions"""
    if len(transactions) < 2:
        return None

    def xnpv(rate):
        first_date = pd.to_datetime(transactions['date'].min())
        days = [(pd.to_datetime(date) - first_date).days for date in transactions['date']]
        return sum([cf * (1 + rate) ** (-d/365.0) for cf, d in zip(transactions['cashflow'], days)])

    def xnpv_der(rate):
        first_date = pd.to_datetime(transactions['date'].min())
        days = [(pd.to_datetime(date) - first_date).days for date in transactions['date']]
        return sum([cf * (-d/365.0) * (1 + rate) ** (-d/365.0 - 1) 
                   for cf, d in zip(transactions['cashflow'], days)])

    try:
        return newton(xnpv, x0=0.1, fprime=xnpv_der, maxiter=1000)
    except:
        return None

def calculate_portfolio_weights(df, latest_nav):
    """Calculate current portfolio weights for each scheme"""
    df = df.groupby(['scheme_name', 'code']).agg({
        'units': 'sum'
    }).reset_index()
    
    df = df[df['units'] > 0]
    df = df.merge(latest_nav[['code', 'nav_value']], on='code', how='left')
    df['current_value'] = df['units'] * df['nav_value']
    total_value = df['current_value'].sum()
    df['weight'] = (df['current_value'] / total_value * 100) if total_value > 0 else 0
    
    return df

def calculate_xirr(df, latest_nav):
    """Calculate XIRR for portfolio and individual schemes"""
    schemes = df['scheme_name'].unique()
    xirr_results = {}
    portfolio_growth = []

    for scheme in schemes:
        scheme_data = df[df['scheme_name'] == scheme].copy()
        if not scheme_data.empty:
            scheme_nav = latest_nav[latest_nav['code'] == scheme_data['code'].iloc[0]]
            if not scheme_nav.empty:
                latest_value = scheme_data['units'].sum() * scheme_nav['nav_value'].iloc[0]
                final_cf = pd.DataFrame({
                    'date': [datetime.now()],
                    'cashflow': [latest_value]
                })
                scheme_cashflows = scheme_data[['date', 'cashflow']]
                total_cashflows = pd.concat([scheme_cashflows, final_cf])
                rate = xirr(total_cashflows)
                xirr_results[scheme_data['code'].iloc[0]] = round(rate * 100, 1) if rate is not None else 0

    unique_dates = sorted(df['date'].unique())
    
    for date in unique_dates:
        transactions_to_date = df[df['date'] <= date].copy()
        transactions_to_date = transactions_to_date.merge(latest_nav[['code', 'nav_value']], on='code', how='left')
        transactions_to_date['current_value'] = transactions_to_date['units'] * transactions_to_date['nav_value']
        total_value = transactions_to_date.groupby('date')['current_value'].sum().loc[date]
        portfolio_growth.append({'date': date, 'value': total_value})

    if not df.empty:
        total_value = df.merge(latest_nav[['code', 'nav_value']], on='code', how='left')
        total_value['current_value'] = total_value['units'] * total_value['nav_value']
        final_value = pd.DataFrame({
            'date': [datetime.now()],
            'cashflow': [total_value['current_value'].sum()]
        })
        total_cashflows = pd.concat([df[['date', 'cashflow']], final_value])
        portfolio_xirr = xirr(total_cashflows)
        xirr_results['Portfolio'] = round(portfolio_xirr * 100, 1) if portfolio_xirr is not None else 0

    return xirr_results, pd.DataFrame(portfolio_growth)

def calculate_returns(nav_data, portfolio_funds):
    """Calculate historical returns for portfolio funds"""
    nav_data = nav_data[nav_data['code'].isin(portfolio_funds)]
    nav_pivot = nav_data.pivot(index='date', columns='code', values='nav_value')
    daily_returns = nav_pivot.pct_change()
    monthly_returns = nav_pivot.resample('M').last().pct_change()
    
    return daily_returns, monthly_returns

def calculate_portfolio_metrics(weights_df, returns_df):
    """Calculate portfolio risk metrics"""
    weights = weights_df.set_index('code')['weight'] / 100
    returns_df = returns_df[weights.index]
    
    monthly_cov_matrix = returns_df.cov()
    monthly_corr_matrix = returns_df.corr()
    monthly_fund_volatilities = returns_df.std()
    yearly_fund_volatilities = monthly_fund_volatilities * np.sqrt(12)
    
    portfolio_variance_monthly = np.dot(weights.T, np.dot(monthly_cov_matrix, weights))
    portfolio_std_monthly = np.sqrt(portfolio_variance_monthly)
    
    portfolio_variance_yearly = portfolio_variance_monthly * 12
    portfolio_std_yearly = portfolio_std_monthly * np.sqrt(12)
    
    risk_contribution = np.multiply(weights, np.dot(monthly_cov_matrix, weights)) / portfolio_variance_monthly
    weighted_volatility = np.sum(weights * monthly_fund_volatilities)
    diversification_ratio = weighted_volatility / portfolio_std_monthly
    
    return {
        'covariance_matrix': monthly_cov_matrix,
        'correlation_matrix': monthly_corr_matrix,
        'portfolio_variance_monthly': portfolio_variance_monthly,
        'portfolio_std_monthly': portfolio_std_monthly,
        'portfolio_variance_yearly': portfolio_variance_yearly,
        'portfolio_std_yearly': portfolio_std_yearly,
        'monthly_fund_volatilities': monthly_fund_volatilities,
        'yearly_fund_volatilities': yearly_fund_volatilities,
        'risk_contribution': risk_contribution,
        'diversification_ratio': diversification_ratio
    }

def interpret_portfolio_metrics(risk_metrics, weights_df, xirr_results):
    """Generate insights from portfolio metrics"""
    insights = []
    
    portfolio_xirr = xirr_results['Portfolio']
    portfolio_risk = risk_metrics['portfolio_std_yearly'] * 100
    risk_return_ratio = portfolio_xirr / portfolio_risk if portfolio_risk > 0 else 0
    
    div_ratio = risk_metrics['diversification_ratio']
    correlation_matrix = risk_metrics['correlation_matrix']
    avg_correlation = correlation_matrix.values[np.triu_indices_from(correlation_matrix.values, k=1)].mean()
    
    insights.append({
        'category': 'Risk-Return Efficiency',
        'insight': f"Portfolio generates {portfolio_xirr:.1f}% return per {portfolio_risk:.1f}% of risk (yearly volatility). " +
                  f"Risk-return ratio: {risk_return_ratio:.2f}x",
        'recommendation': "Consider rebalancing if ratio is below your target risk-return efficiency."
    })
    
    insights.append({
        'category': 'Diversification Quality',
        'insight': f"Diversification ratio: {div_ratio:.2f}x. Average correlation between funds: {avg_correlation:.2f}",
        'recommendation': "Ratio > 1.5x suggests good diversification; < 1.2x might need attention."
    })
    
    top_weight = weights_df['weight'].max()
    top_fund = weights_df.loc[weights_df['weight'].idxmax(), 'scheme_name']
    
    insights.append({
        'category': 'Concentration Risk',
        'insight': f"Highest allocation: {top_weight:.1f}% in {top_fund}",
        'recommendation': "Consider rebalancing if any single fund exceeds 25% of portfolio."
    })
    
    fund_vols = risk_metrics['yearly_fund_volatilities'] * 100
    high_vol_funds = fund_vols[fund_vols > risk_metrics['portfolio_std_yearly'] * 100 * 1.2]
    
    if not high_vol_funds.empty:
        funds_list = ', '.join(weights_df.set_index('code').loc[high_vol_funds.index, 'scheme_name'].tolist())
        insights.append({
            'category': 'Volatility Analysis',
            'insight': f"Funds with significantly higher volatility: {funds_list}",
            'recommendation': "Review if high-volatility exposure aligns with investment goals."
        })
    
    return insights

def main():
    """
    Main function to run the Portfolio Risk Analysis Dashboard.
    This function sets up the Streamlit page configuration, retrieves and processes portfolio data,
    calculates various risk and return metrics, and displays the results in an interactive dashboard.
    Sections displayed in the dashboard:
    1. Portfolio Composition
    2. Fund-wise Analysis
    3. Fund Correlations
    4. Portfolio Risk Metrics
    5. Portfolio Insights
    6. Additional Portfolio Statistics
    The function handles various scenarios such as missing data and provides recommendations for portfolio rebalancing based on correlation metrics.
    Raises:
        Exception: If any error occurs during data retrieval or processing, an error message is displayed.
    Returns:
        None
    """
    st.set_page_config(page_title="Portfolio Risk Analysis", layout="wide")
    st.title("Portfolio Risk Analysis Dashboard")

    try:
        df = get_portfolio_data()
        
        if df.empty:
            st.warning("No portfolio data found.")
            return

        portfolio_funds = get_portfolio_funds(df)
        
        if not portfolio_funds:
            st.warning("No active funds found in portfolio.")
            return

        latest_nav = get_latest_nav(portfolio_funds)
        historical_nav = get_historical_nav(portfolio_funds)

        if latest_nav.empty or historical_nav.empty:
            st.warning("No NAV data found for portfolio funds.")
            return

        df['date'] = pd.to_datetime(df['date'])
        df = prepare_cashflows(df)
        historical_nav['date'] = pd.to_datetime(historical_nav['date'])

        xirr_results, portfolio_growth_df = calculate_xirr(df, latest_nav)
        weights_df = calculate_portfolio_weights(df, latest_nav)
        daily_returns, monthly_returns = calculate_returns(historical_nav, portfolio_funds)
        risk_metrics = calculate_portfolio_metrics(weights_df, monthly_returns)

        # Display sections
        st.header("1. Portfolio Composition")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Current Portfolio Value")
            st.metric("Total Value", format_indian_number(weights_df['current_value'].sum()))
            
        with col2:
            st.subheader("Portfolio XIRR")
            st.metric("XIRR", f"{xirr_results['Portfolio']:.1f}%")

        st.header("2. Fund-wise Analysis")
        fund_analysis = weights_df.copy()
        fund_analysis['Weight (%)'] = fund_analysis['weight'].round(2)
        fund_analysis['Monthly Volatility (%)'] = fund_analysis['code'].map(
            risk_metrics['monthly_fund_volatilities'] * 100
        ).round(2)
        fund_analysis['Yearly Volatility (%)'] = fund_analysis['code'].map(
            risk_metrics['yearly_fund_volatilities'] * 100
        ).round(2)
        fund_analysis['XIRR (%)'] = fund_analysis['code'].map(xirr_results)
        fund_analysis['sort_value'] = fund_analysis['current_value']
        fund_analysis['Current Value'] = fund_analysis['current_value'].apply(format_indian_number)

        display_columns = [
            'scheme_name', 
            'Weight (%)', 
            'Monthly Volatility (%)',
            'Yearly Volatility (%)',
            'XIRR (%)', 
            'Current Value'
        ]

        sortable_df = fund_analysis[display_columns].copy()
        sortable_df['sort_value'] = fund_analysis['sort_value']
        st.dataframe(
            sortable_df.set_index('sort_value')[display_columns].sort_index(ascending=False)
        )

        # Correlation Matrix
        st.header("3. Fund Correlations")
        correlation_display = risk_metrics['correlation_matrix'].copy()
        correlation_display.index = weights_df.set_index('code')['scheme_name']
        correlation_display.columns = weights_df.set_index('code')['scheme_name']
        
        st.dataframe(
            correlation_display.style.format("{:.2f}")
            .background_gradient(cmap='RdYlGn', vmin=-1, vmax=1)
        )

        st.subheader("How to Interpret the Correlation Matrix")
        
        avg_correlation = correlation_display.values[np.triu_indices_from(correlation_display.values, k=1)].mean()
        max_correlation = correlation_display.values[np.triu_indices_from(correlation_display.values, k=1)].max()
        min_correlation = correlation_display.values[np.triu_indices_from(correlation_display.values, k=1)].min()
        
        high_corr_threshold = 0.7
        high_corr_pairs = []
        for i in range(len(correlation_display.index)):
            for j in range(i+1, len(correlation_display.columns)):
                if correlation_display.iloc[i,j] >= high_corr_threshold:
                    high_corr_pairs.append((
                        correlation_display.index[i],
                        correlation_display.columns[j],
                        correlation_display.iloc[i,j]
                    ))

        st.write("""
        **Understanding Correlation Values:**
        - 1.00: Perfect positive correlation
        - 0.70 to 0.99: Strong positive correlation
        - 0.30 to 0.69: Moderate positive correlation
        - -0.29 to 0.29: Weak or no correlation
        - -0.69 to -0.30: Moderate negative correlation
        - -0.99 to -0.70: Strong negative correlation
        - -1.00: Perfect negative correlation
        """)

        st.write("**Portfolio Correlation Statistics:**")
        st.write(f"- Average correlation: {avg_correlation:.2f}")
        st.write(f"- Highest correlation: {max_correlation:.2f}")
        st.write(f"- Lowest correlation: {min_correlation:.2f}")

        if high_corr_pairs:
            st.write("\n**Highly Correlated Fund Pairs (>0.70):**")
            for fund1, fund2, corr in high_corr_pairs:
                st.write(f"- {fund1} and {fund2}: {corr:.2f}")

        if avg_correlation > 0.6:
            st.warning("⚠️ Your portfolio shows relatively high average correlation. Consider adding funds with lower correlation to improve diversification.")
            
            avg_correlations = {}
            for fund in correlation_display.index:
                correlations = correlation_display.loc[fund].drop(fund)
                avg_correlations[fund] = correlations.mean()
            
            fund_correlations = pd.DataFrame.from_dict(avg_correlations, orient='index', 
                                                     columns=['avg_correlation'])
            fund_correlations['weight'] = weights_df.set_index('scheme_name')['weight']
            
            high_corr_funds = fund_correlations[
                (fund_correlations['avg_correlation'] > 0.65) & 
                (fund_correlations['weight'] > 5)
            ].sort_values('avg_correlation', ascending=False)
            
            low_corr_funds = fund_correlations[
                fund_correlations['avg_correlation'] < fund_correlations['avg_correlation'].mean()
            ].sort_values('avg_correlation')
            
            st.subheader("Portfolio Rebalancing Suggestions")
            
            if not high_corr_funds.empty:
                st.write("**Funds to Consider Reducing Exposure:**")
                reduce_exposure_data = []
                for fund in high_corr_funds.index:
                    corr_pairs = correlation_display[fund].sort_values(ascending=False)[1:4]
                    highly_corr_with = "; ".join([f"{f} ({v:.2f})" for f, v in corr_pairs.items()])
                    
                    reduce_exposure_data.append({
                        'Fund Name': fund,
                        'Current Weight (%)': high_corr_funds.loc[fund, 'weight'],
                        'Avg Correlation': high_corr_funds.loc[fund, 'avg_correlation'],
                        'Highly Correlated With': highly_corr_with
                    })
                
                reduce_df = pd.DataFrame(reduce_exposure_data)
                st.dataframe(
                    reduce_df.style.format({
                        'Current Weight (%)': '{:.1f}',
                        'Avg Correlation': '{:.2f}'
                    }).background_gradient(
                        subset=['Avg Correlation'],
                        cmap='RdYlGn_r'
                    )
                )
            
            if not low_corr_funds.empty:
                st.write("\n**Funds to Consider Increasing Exposure:**")
                increase_exposure_data = []
                for fund in low_corr_funds.index:
                    corr_pairs = correlation_display[fund].sort_values()[1:4]
                    least_corr_with = "; ".join([f"{f} ({v:.2f})" for f, v in corr_pairs.items()])
                    
                    increase_exposure_data.append({
                        'Fund Name': fund,
                        'Current Weight (%)': low_corr_funds.loc[fund, 'weight'],
                        'Avg Correlation': low_corr_funds.loc[fund, 'avg_correlation'],
                        'Least Correlated With': least_corr_with
                    })
                
                increase_df = pd.DataFrame(increase_exposure_data)
                st.dataframe(
                    increase_df.style.format({
                        'Current Weight (%)': '{:.1f}',
                        'Avg Correlation': '{:.2f}'
                    }).background_gradient(
                        subset=['Avg Correlation'],
                        cmap='RdYlGn'
                    )
                )
            
            st.write("""
            **Rebalancing Guidelines:**
            1. Consider gradually reducing exposure to highly correlated funds while maintaining sector/asset class exposure
            2. Look to increase allocation to funds showing lower correlation with the rest of your portfolio
            3. Aim for position sizes that balance diversification with meaningful impact on portfolio returns
            4. Consider tax implications and exit loads before making significant changes
            """)
            
            current_portfolio_corr = avg_correlation
            suggested_reduction = high_corr_funds['weight'].sum() * 0.3
            
            impact_data = pd.DataFrame({
                'Metric': [
                    'Current Portfolio Average Correlation',
                    'Suggested Reduction in Highly Correlated Funds',
                    'Target Portfolio Correlation'
                ],
                'Value': [
                    f"{current_portfolio_corr:.2f}",
                    f"{suggested_reduction:.1f}%",
                    "< 0.60"
                ]
            })
            
            st.write("\n**Potential Impact of Rebalancing:**")
            st.dataframe(impact_data)
            
        elif avg_correlation < 0.3:
            st.success("✅ Your portfolio shows good diversification based on correlation metrics.")
        else:
            st.info("ℹ️ Your portfolio shows moderate correlation levels. Consider monitoring for opportunities to improve diversification further.")

        # Portfolio Risk Metrics
        st.header("4. Portfolio Risk Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="Monthly Volatility", 
                value=f"{risk_metrics['portfolio_std_monthly'] * 100:.2f}%"
            )
        
        with col2:
            st.metric(
                label="Yearly Volatility",
                value=f"{risk_metrics['portfolio_std_yearly'] * 100:.2f}%"
            )
        
        with col3:
            st.metric(
                label="Diversification Ratio",
                value=f"{risk_metrics['diversification_ratio']:.2f}x"
            )
        

        # Portfolio Insights
        st.header("5. Portfolio Insights")
        insights = interpret_portfolio_metrics(risk_metrics, weights_df, xirr_results)
        
        for insight in insights:
            with st.expander(f"{insight['category']}"):
                st.write("📊 **Analysis:**", insight['insight'])
                st.write("💡 **Recommendation:**", insight['recommendation'])

        # Additional Portfolio Statistics
        st.header("6. Additional Portfolio Statistics")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Risk Metrics Summary")
            risk_summary = pd.DataFrame({
                'Metric': [
                    'Portfolio Monthly Volatility',
                    'Portfolio Yearly Volatility',
                    'Diversification Ratio',
                    'Average Fund Correlation'
                ],
                'Value': [
                    f"{risk_metrics['portfolio_std_monthly'] * 100:.2f}%",
                    f"{risk_metrics['portfolio_std_yearly'] * 100:.2f}%",
                    f"{risk_metrics['diversification_ratio']:.2f}x",
                    f"{risk_metrics['correlation_matrix'].values[np.triu_indices_from(risk_metrics['correlation_matrix'].values, k=1)].mean():.2f}"
                ]
            })
            st.dataframe(risk_summary)
        
        with col2:
            st.subheader("Return Metrics Summary")
            code_to_scheme = weights_df.set_index('code')['scheme_name'].to_dict()
            
            best_fund_code = max([(k, v) for k, v in xirr_results.items() if k != 'Portfolio'], 
                               key=lambda x: x[1], default=('N/A', 0))[0]
            worst_fund_code = min([(k, v) for k, v in xirr_results.items() if k != 'Portfolio'], 
                                key=lambda x: x[1], default=('N/A', 0))[0]
            
            best_fund_name = code_to_scheme.get(best_fund_code, 'N/A')
            worst_fund_name = code_to_scheme.get(worst_fund_code, 'N/A')
            
            returns_summary = pd.DataFrame({
                'Metric': [
                    'Portfolio XIRR',
                    'Best Performing Fund',
                    'Worst Performing Fund',
                    'Average Fund XIRR'
                ],
                'Value': [
                    f"{xirr_results.get('Portfolio', 0):.1f}%",
                    f"{best_fund_name} ({xirr_results.get(best_fund_code, 0):.1f}%)",
                    f"{worst_fund_name} ({xirr_results.get(worst_fund_code, 0):.1f}%)",
                    f"{np.mean([v for k, v in xirr_results.items() if k != 'Portfolio']):.1f}%"
                ]
            })
            st.dataframe(returns_summary)

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.error("Please check your database connection and data integrity.")

if __name__ == "__main__":
    main()