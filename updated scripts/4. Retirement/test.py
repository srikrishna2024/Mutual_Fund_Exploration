import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Helper functions for Indian number formatting
def format_indian_number(number):
    """Convert a number to Indian format (lakhs and crores)"""
    abs_number = abs(number)
    if abs_number >= 10000000:  # Crores
        return f"₹{abs_number/10000000:.2f} Cr"
    elif abs_number >= 100000:  # Lakhs
        return f"₹{abs_number/100000:.2f} L"
    else:
        return f"₹{abs_number:,.0f}"

def format_indian_currency(number):
    """Format number to Indian currency format with commas"""
    s = str(number)
    if len(s) > 3:
        last_3 = s[-3:]
        other = s[:-3]
        if len(other) > 2:
            groups = []
            for i in range(len(other)-2, -1, -2):
                if i == 0:
                    groups.append(other[0:i+2])
                else:
                    groups.append(other[i:i+2])
            groups.reverse()
            other = ','.join(groups)
        return f"₹{other},{last_3}"
    return f"₹{s}"

def generate_return_sequences(years, mean_return, std_dev, num_sequences=1000):
    """Generate multiple return sequences using Monte Carlo simulation"""
    return np.random.normal(mean_return, std_dev, size=(num_sequences, years))

def calculate_portfolio_paths(initial_amount, annual_contribution, years, return_sequences):
    """Calculate multiple portfolio paths given return sequences"""
    paths = np.zeros((len(return_sequences), years))
    paths[:, 0] = initial_amount
    
    for i in range(1, years):
        paths[:, i] = (paths[:, i-1] * (1 + return_sequences[:, i-1]) + 
                      annual_contribution)
    
    return paths

def get_retirement_success_metrics(portfolio_paths, annual_expenses, retirement_year_index):
    """Calculate success metrics for retirement planning"""
    success_paths = 0
    total_paths = len(portfolio_paths)
    
    for path in portfolio_paths:
        # Check if portfolio sustains expenses throughout retirement
        retirement_portfolio = path[retirement_year_index:]
        annual_withdrawals = np.array([annual_expenses * (1.06 ** i) 
                                     for i in range(len(retirement_portfolio))])
        
        remaining_portfolio = retirement_portfolio.copy()
        for i in range(1, len(remaining_portfolio)):
            remaining_portfolio[i] = (remaining_portfolio[i-1] * (1 + 0.08) - 
                                    annual_withdrawals[i-1])
        
        if all(remaining_portfolio >= 0):
            success_paths += 1
    
    success_rate = (success_paths / total_paths) * 100
    return success_rate

def calculate_recommended_allocation(age, retirement_age):
    """Calculate recommended equity-debt allocation based on age"""
    years_to_retirement = retirement_age - age
    if years_to_retirement > 20:
        equity_ratio = 0.80
    elif years_to_retirement > 10:
        equity_ratio = 0.70
    elif years_to_retirement > 5:
        equity_ratio = 0.60
    else:
        equity_ratio = 0.40
    return equity_ratio, 1 - equity_ratio

def calculate_retirement_cashflows(current_age, retirement_age, life_expectancy,
                                current_salary, salary_growth_rate,
                                current_expenses, inflation_rate,
                                current_equity, current_debt,
                                annual_equity_investment, annual_debt_investment,
                                equity_growth_rate, debt_growth_rate,
                                retirement_income_ratio):
    """Calculate year-by-year retirement cashflows"""
    years = life_expectancy - current_age + 1
    ages = list(range(current_age, life_expectancy + 1))
    
    # Initialize arrays
    salary = np.zeros(years)
    other_income = np.zeros(years)
    expenses = np.zeros(years)
    withdrawals = np.zeros(years)
    equity_balance = np.zeros(years)
    debt_balance = np.zeros(years)
    
    # Set initial values
    salary[0] = current_salary
    expenses[0] = current_expenses
    equity_balance[0] = current_equity
    debt_balance[0] = current_debt
    
    # Calculate year-by-year values
    for i in range(1, years):
        # Pre-retirement
        if ages[i] < retirement_age:
            salary[i] = salary[i-1] * (1 + salary_growth_rate)
            equity_balance[i] = (equity_balance[i-1] * (1 + equity_growth_rate) + 
                               annual_equity_investment)
            debt_balance[i] = (debt_balance[i-1] * (1 + debt_growth_rate) + 
                             annual_debt_investment)
        # Post-retirement
        else:
            salary[i] = 0
            other_income[i] = salary[i-1] * retirement_income_ratio
            withdrawals[i] = expenses[i-1] * (1 + inflation_rate) - other_income[i]
            
            # Update portfolio balances
            total_portfolio = equity_balance[i-1] + debt_balance[i-1]
            if total_portfolio > 0:
                equity_ratio = equity_balance[i-1] / total_portfolio
                debt_ratio = debt_balance[i-1] / total_portfolio
                
                equity_balance[i] = (equity_balance[i-1] * (1 + equity_growth_rate) - 
                                   withdrawals[i] * equity_ratio)
                debt_balance[i] = (debt_balance[i-1] * (1 + debt_growth_rate) - 
                                 withdrawals[i] * debt_ratio)
        
        expenses[i] = expenses[i-1] * (1 + inflation_rate)
    
    # Calculate net cashflow
    income = salary + other_income
    net_cashflow = income - expenses - withdrawals
    total_portfolio = equity_balance + debt_balance
    
    # Create DataFrame
    df = pd.DataFrame({
        'Age': ages,
        'Salary': salary,
        'Other Income': other_income,
        'Expenses': expenses,
        'Withdrawals': withdrawals,
        'Net Cashflow': net_cashflow,
        'Equity Balance': equity_balance,
        'Debt Balance': debt_balance,
        'Total Portfolio': total_portfolio
    })
    
    return df
def calculate_portfolio_with_dynamic_allocation(current_age, retirement_age, life_expectancy,
                                             current_salary, salary_growth_rate,
                                             current_expenses, inflation_rate,
                                             current_equity, current_debt,
                                             annual_equity_investment, annual_debt_investment,
                                             equity_returns, debt_returns):
    """Calculate portfolio values with dynamic allocation to reduce sequence risk"""
    years = life_expectancy - current_age + 1
    ages = list(range(current_age, life_expectancy + 1))
    
    # Initialize arrays
    equity_balance = np.zeros(years)
    debt_balance = np.zeros(years)
    
    # Set initial values
    equity_balance[0] = current_equity
    debt_balance[0] = current_debt
    
    for i in range(1, years):
        age = ages[i]
        # Adjust allocation based on years to retirement
        years_to_retirement = retirement_age - age
        if years_to_retirement > 0:
            equity_ratio = min(0.75, max(0.25, years_to_retirement/40))
        else:
            equity_ratio = 0.4  # Post retirement allocation
        
        # Calculate year's investments with dynamic allocation
        year_equity_investment = annual_equity_investment * equity_ratio
        year_debt_investment = annual_debt_investment * (1 - equity_ratio)
        
        # Update balances with returns and new investments
        equity_balance[i] = equity_balance[i-1] * (1 + equity_returns[i-1]) + year_equity_investment
        debt_balance[i] = debt_balance[i-1] * (1 + debt_returns[i-1]) + year_debt_investment
    
    return pd.DataFrame({
        'Age': ages,
        'Equity Balance': equity_balance,
        'Debt Balance': debt_balance,
        'Total Portfolio': equity_balance + debt_balance
    })

# Main UI
st.set_page_config(page_title="Enhanced Indian Retirement Calculator", layout="wide")
st.title('Enhanced Indian Retirement Calculator')
st.write("Plan your retirement with Monte Carlo simulation and risk analysis")

# Input parameters in tabs
tab1, tab2, tab3 = st.tabs(["Basic Info", "Investment Details", "Retirement Planning"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        current_age = st.number_input('Current Age', min_value=20, max_value=90, value=30)
        current_salary = st.number_input('Current Annual Salary (₹)', 
                                       min_value=0, 
                                       value=1000000,
                                       step=100000,
                                       help="Enter annual salary in Rupees")
        salary_growth_rate = st.number_input('Annual Salary Growth Rate (%)', 
                                           min_value=0.0, 
                                           max_value=20.0, 
                                           value=8.0) / 100
    
    with col2:
        current_expenses = st.number_input('Current Annual Expenses (₹)', 
                                         min_value=0, 
                                         value=700000,
                                         step=50000)
        inflation_rate = st.number_input('Inflation Rate (%)', 
                                       min_value=0.0, 
                                       max_value=20.0, 
                                       value=6.0) / 100

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        current_equity = st.number_input('Current Equity Investments (₹)', 
                                       min_value=0, 
                                       value=2000000,
                                       step=100000)
        current_debt = st.number_input('Current Debt Investments (₹)', 
                                     min_value=0, 
                                     value=1000000,
                                     step=100000)
        annual_equity_investment = st.number_input('Annual Equity Investment (₹)', 
                                                 min_value=0, 
                                                 value=300000,
                                                 step=50000)
        annual_debt_investment = st.number_input('Annual Debt Investment (₹)', 
                                               min_value=0, 
                                               value=200000,
                                               step=50000)
    
    with col2:
        equity_growth_rate = st.number_input('Equity Growth Rate (%)', 
                                           min_value=0.0, 
                                           max_value=20.0, 
                                           value=12.0) / 100
        debt_growth_rate = st.number_input('Debt Growth Rate (%)', 
                                         min_value=0.0, 
                                         max_value=20.0, 
                                         value=7.0) / 100

with tab3:
    col1, col2 = st.columns(2)
    with col1:
        retirement_age = st.number_input('Retirement Age', 
                                       min_value=current_age+1, 
                                       max_value=90, 
                                       value=60)
        life_expectancy = st.number_input('Life Expectancy', 
                                        min_value=retirement_age+1, 
                                        max_value=100, 
                                        value=85)
    
    with col2:
        retirement_income_ratio = st.number_input('Other Income After Retirement (as % of Last Salary)', 
                                                min_value=0.0, 
                                                max_value=100.0, 
                                                value=20.0) / 100

# New section for interactive investment adjustments
st.subheader("Adjust Investment Allocations")
col1, col2 = st.columns(2)
with col1:
    adjusted_annual_equity = st.slider('Annual Equity Investment (₹)', 
                                     min_value=0, 
                                     max_value=1000000, 
                                     value=annual_equity_investment,
                                     step=10000)
with col2:
    adjusted_annual_debt = st.slider('Annual Debt Investment (₹)', 
                                   min_value=0, 
                                   max_value=1000000, 
                                   value=annual_debt_investment,
                                   step=10000)

if st.button('Calculate Projections'):
    # Generate base return sequences
    years_to_simulate = life_expectancy - current_age
    equity_returns = generate_return_sequences(years_to_simulate, equity_growth_rate, 0.18)[0]
    debt_returns = generate_return_sequences(years_to_simulate, debt_growth_rate, 0.06)[0]
    
    # Calculate base projections
    base_df = calculate_retirement_cashflows(
        current_age, retirement_age, life_expectancy,
        current_salary, salary_growth_rate,
        current_expenses, inflation_rate,
        current_equity, current_debt,
        annual_equity_investment, annual_debt_investment,
        equity_growth_rate, debt_growth_rate,
        retirement_income_ratio
    )
    
    # Calculate dynamic allocation projections
    dynamic_df = calculate_portfolio_with_dynamic_allocation(
        current_age, retirement_age, life_expectancy,
        current_salary, salary_growth_rate,
        current_expenses, inflation_rate,
        current_equity, current_debt,
        adjusted_annual_equity, adjusted_annual_debt,
        equity_returns, debt_returns
    )
    
    # Plot 1: Base Projections
    fig1 = go.Figure()
    
    fig1.add_trace(go.Scatter(x=base_df['Age'], y=base_df['Salary'] + base_df['Other Income'],
                             name='Total Income', line=dict(color='green')))
    fig1.add_trace(go.Scatter(x=base_df['Age'], y=base_df['Expenses'],
                             name='Expenses', line=dict(color='red')))
    fig1.add_trace(go.Scatter(x=base_df['Age'], y=base_df['Equity Balance'],
                             name='Equity Investments', line=dict(color='blue')))
    fig1.add_trace(go.Scatter(x=base_df['Age'], y=base_df['Debt Balance'],
                             name='Debt Investments', line=dict(color='orange')))
    
    fig1.update_layout(
        title='Current Projections',
        xaxis_title='Age',
        yaxis_title='Amount (₹)',
        yaxis_type='log',
        height=500
    )
    
    # Plot 2: Dynamic Allocation
    fig2 = go.Figure()
    
    fig2.add_trace(go.Scatter(x=dynamic_df['Age'], y=dynamic_df['Equity Balance'],
                             name='Suggested Equity', line=dict(color='blue')))
    fig2.add_trace(go.Scatter(x=dynamic_df['Age'], y=dynamic_df['Debt Balance'],
                             name='Suggested Debt', line=dict(color='orange')))
    fig2.add_trace(go.Scatter(x=base_df['Age'], y=base_df['Expenses'],
                             name='Expenses', line=dict(color='red')))
    
    fig2.update_layout(
        title='Suggested Dynamic Allocation',
        xaxis_title='Age',
        yaxis_title='Amount (₹)',
        yaxis_type='log',
        height=500
    )
    
    # Plot 3: Retirement Sufficiency Analysis
    fig3 = go.Figure()
    
    retirement_expenses = base_df.loc[base_df['Age'] >= retirement_age, 'Expenses']
    current_portfolio = base_df.loc[base_df['Age'] >= retirement_age, 'Total Portfolio']
    suggested_portfolio = dynamic_df.loc[dynamic_df['Age'] >= retirement_age, 'Total Portfolio']
    
    fig3.add_trace(go.Scatter(x=retirement_expenses.index, y=retirement_expenses,
                             name='Required Corpus', line=dict(color='red')))
    fig3.add_trace(go.Scatter(x=current_portfolio.index, y=current_portfolio,
                             name='Current Plan', line=dict(color='blue')))
    fig3.add_trace(go.Scatter(x=suggested_portfolio.index, y=suggested_portfolio,
                             name='Suggested Plan', line=dict(color='green')))
    
    fig3.update_layout(
        title='Retirement Corpus Sufficiency',
        xaxis_title='Age',
        yaxis_title='Amount (₹)',
        yaxis_type='log',
        height=500
    )
    
    # Display plots
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)
    st.plotly_chart(fig3, use_container_width=True)
    
    # Add insights
    st.subheader("Analysis Insights")
    
    current_total = base_df.loc[base_df['Age'] == retirement_age, 'Total Portfolio'].iloc[0]
    suggested_total = dynamic_df.loc[dynamic_df['Age'] == retirement_age, 'Total Portfolio'].iloc[0]
    required_expenses = base_df.loc[base_df['Age'] == retirement_age, 'Expenses'].iloc[0] * 25
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Current Plan Corpus at Retirement", 
                 format_indian_number(current_total))
    with col2:
        st.metric("Suggested Plan Corpus at Retirement", 
                 format_indian_number(suggested_total))
    with col3:
        st.metric("Required Corpus (25x Annual Expenses)", 
                 format_indian_number(required_expenses))
    
    if current_total < required_expenses:
        st.warning(f"Your current investment plan may not be sufficient for retirement. Consider increasing your investments by {format_indian_number((required_expenses - current_total)/years_to_simulate)} per year.")
    else:
        st.success("Your current investment plan appears to be on track for retirement.")

# Footer
st.markdown("""
---
### Important Notes:
- All calculations are in Indian Rupees (₹)
- The Monte Carlo simulation accounts for sequence of returns risk
- Historical volatility assumptions: Equity (18%), Debt (6%)
- Consider consulting with a SEBI registered financial advisor
- Returns are based on historical market performance and may vary
- Tax implications (including LTCG, STCG) are not considered
- EPF, PPF, and other government schemes should be considered separately
- The simulation uses simplified assumptions and should be used as a general guide only
""")