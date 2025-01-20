import streamlit as st
import pandas as pd
import psycopg
import numpy as np
from datetime import datetime, date
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

def format_indian_currency(amount):
    """Format amount in lakhs with 2 decimal places"""
    amount_in_lakhs = float(amount) / 100000
    return f"₹{amount_in_lakhs:.2f}L"

def get_goals():
    """Get list of unique goals from goals table"""
    with connect_to_db() as conn:
        query = "SELECT DISTINCT goal_name FROM goals ORDER BY goal_name"
        return pd.read_sql(query, conn)['goal_name'].tolist()

def get_current_investments(goal_name):
    """Get current equity and debt investments for the goal"""
    with connect_to_db() as conn:
        query = """
        SELECT 
            investment_type,
            SUM(current_value) as total_value
        FROM goals 
        WHERE goal_name = %s
        GROUP BY investment_type
        """
        df = pd.read_sql(query, conn, params=[goal_name])
        
        # Convert to dictionary with types as keys
        investments = {row['investment_type']: row['total_value'] for _, row in df.iterrows()}
        
        return {
            'equity': investments.get('Equity', 0),
            'debt': investments.get('Debt', 0)
        }

def calculate_future_value(present_value, years, inflation_rate):
    """Calculate future value considering inflation"""
    return present_value * (1 + inflation_rate/100) ** years

def calculate_retirement_corpus(annual_expenses, life_expectancy, retirement_age, years_to_retirement, inflation_rate, post_ret_return=6):
    """Calculate required retirement corpus with corrected calculation"""
    # Calculate expenses at retirement
    expenses_at_retirement = calculate_future_value(annual_expenses, years_to_retirement, inflation_rate)
    
    # Calculate years in retirement
    retirement_years = life_expectancy - retirement_age
    
    # Calculate required corpus using PMT formula in reverse
    # We need to solve for PV where:
    # PV = PMT * (1 - (1 + r)^-n) / r
    # where r is post-retirement return rate adjusted for inflation
    real_return = (1 + post_ret_return/100) / (1 + inflation_rate/100) - 1
    
    if abs(real_return) < 0.0001:
        corpus = expenses_at_retirement * retirement_years
    else:
        corpus = expenses_at_retirement * (1 - (1 + real_return) ** -retirement_years) / real_return
    
    # Add 10% buffer
    corpus *= 1.1
    
    return corpus

def calculate_required_investment(target_amount, current_amount, years, expected_return, yearly_increase):
    """Calculate yearly investment required with increasing contributions"""
    if years <= 0:
        return 0
        
    # Convert percentages to decimals
    r = expected_return / 100
    g = yearly_increase / 100
    
    # Calculate future value of current investments
    future_value_current = current_amount * (1 + r) ** years
    
    # Amount needed from new investments
    additional_needed = target_amount - future_value_current
    
    if additional_needed <= 0:
        return 0
        
    # Calculate using increasing payment formula
    if abs(r - g) < 0.0001:  # If rates are very close
        pmt = additional_needed / (years * (1 + r) ** (years - 1))
    else:
        pmt = additional_needed * (r - g) / ((1 + g) * ((1 + r) ** years - (1 + g) ** years))
    
    return max(0, pmt)

def create_investment_projection_plot(current_cost, inflation_rate, years, 
                                   current_equity, current_debt,
                                   yearly_equity, yearly_debt,
                                   equity_increase, debt_increase,
                                   is_retirement=False):
    """Create projection plot comparing expected vs actual investments"""
    
    # Calculate target amount (for retirement, current_cost is already the corpus needed)
    target_amount = current_cost if is_retirement else calculate_future_value(current_cost, years, inflation_rate)
    
    # Create year range
    years_range = list(range(years + 1))
    
    # Calculate cumulative investments for each year
    equity_values = [current_equity]
    debt_values = [current_debt]
    
    for year in range(1, years + 1):
        # Calculate equity growth with increasing contributions
        prev_equity = equity_values[-1]
        equity_contribution = yearly_equity * (1 + equity_increase/100) ** (year - 1)
        new_equity = prev_equity * 1.12 + equity_contribution  # Assuming 12% equity returns
        equity_values.append(new_equity)
        
        # Calculate debt growth with increasing contributions
        prev_debt = debt_values[-1]
        debt_contribution = yearly_debt * (1 + debt_increase/100) ** (year - 1)
        new_debt = prev_debt * 1.07 + debt_contribution  # Assuming 7% debt returns
        debt_values.append(new_debt)
    
    # Calculate target line
    if is_retirement:
        target_line = [target_amount] * (years + 1)
    else:
        target_line = [current_cost]
        for year in range(1, years + 1):
            target_line.append(calculate_future_value(current_cost, year, inflation_rate))
    
    # Create plot
    fig = go.Figure()
    
    # Add total portfolio line
    total_portfolio = [sum(x) for x in zip(equity_values, debt_values)]
    fig.add_trace(go.Scatter(x=years_range, y=total_portfolio, 
                            name='Total Portfolio', 
                            fill='tonexty',
                            line=dict(color='rgb(0, 100, 80)')))
    
    # Add target line
    fig.add_trace(go.Scatter(x=years_range, y=target_line, 
                            name='Target Amount',
                            line=dict(color='red', dash='dash')))
    
    # Format y-axis values in lakhs
    fig.update_layout(
        title='Retirement Corpus Projection' if is_retirement else 'Investment Projection vs Target',
        xaxis_title='Years',
        yaxis_title='Amount (₹)',
        height=500,
        showlegend=True,
        yaxis=dict(
            tickformat='.2f',
            ticksuffix='L',
            tickprefix='₹'
        )
    )
    
    return fig

def main():
    """
    Main function to run the Investment Goal Planner application.
    This function sets up the Streamlit page configuration, retrieves the list of goals,
    and displays a form for the user to input details about their investment goals.
    Based on the user's input, it calculates the required yearly investments in equity
    and debt to achieve the goal and displays the results along with an investment projection plot.
    The function handles both retirement and non-retirement goals and adjusts the input fields
    and calculations accordingly.
    The following steps are performed:
    1. Set up the Streamlit page configuration and title.
    2. Retrieve the list of goals from the database.
    3. Display a warning if no goals are found.
    4. Create an input form for the user to select a goal and input relevant details.
    5. Retrieve current investments for the selected goal.
    6. Calculate the future value of the goal or retirement corpus needed.
    7. Calculate the required yearly investments in equity and debt.
    8. Display the investment summary and projection plot.
    9. Display detailed goal planning information in a table.
    Note: This function relies on several helper functions such as `get_goals`, `get_current_investments`,
    `calculate_retirement_corpus`, `calculate_future_value`, `calculate_required_investment`,
    `format_indian_currency`, and `create_investment_projection_plot`.
    Returns:
        None
    """
    st.set_page_config(page_title="Goal Planner", layout="wide")
    st.title("Investment Goal Planner")
    
    # Get list of goals
    goals = get_goals()
    
    if not goals:
        st.warning("No goals found in the database. Please add goals first.")
        return
        
    # Create input form
    with st.form("goal_planner_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            selected_goal = st.selectbox("Select Goal", goals)
            
            # Show different inputs based on whether it's retirement or not
            is_retirement = selected_goal.lower() == "retirement"
            
            if is_retirement:
                current_annual_expenses = st.number_input("Current Annual Expenses", min_value=0.0, step=100000.0, value=1200000.0)
                current_age = st.number_input("Current Age", min_value=18, max_value=100, value=30)
                retirement_age = st.number_input("Expected Retirement Age", min_value=current_age, max_value=100, value=60)
                life_expectancy = st.number_input("Life Expectancy", min_value=retirement_age, max_value=100, value=85)
                years_to_goal = retirement_age - current_age  # Fixed calculation
                current_cost = current_annual_expenses  # For retirement calculations
            else:
                current_cost = st.number_input("Current Cost of Goal", min_value=0.0, step=100000.0, value=1000000.0)
                years_to_goal = st.number_input("Years to Goal", min_value=1, max_value=30, step=1, value=5)
                
            inflation_rate = st.number_input("Expected Inflation Rate (%)", min_value=0.0, max_value=20.0, value=6.0)
            
        with col2:
            # Get current investments for selected goal
            current_investments = get_current_investments(selected_goal)
            
            st.metric("Current Equity Investments", format_indian_currency(current_investments['equity']))
            st.metric("Current Debt Investments", format_indian_currency(current_investments['debt']))
            
            equity_increase = st.number_input("Yearly Increase in Equity Investment (%)", min_value=0.0, max_value=50.0, value=10.0)
            debt_increase = st.number_input("Yearly Increase in Debt Investment (%)", min_value=0.0, max_value=50.0, value=10.0)
        
        submitted = st.form_submit_button("Calculate Required Investment")
        
    if submitted:
        if is_retirement:
            # Calculate retirement corpus needed
            future_value = calculate_retirement_corpus(
                current_annual_expenses,
                life_expectancy,
                retirement_age,
                years_to_goal,
                inflation_rate
            )
        else:
            # Calculate future value of goal
            future_value = calculate_future_value(current_cost, years_to_goal, inflation_rate)
        
        # Calculate required yearly investments
        required_equity = calculate_required_investment(
            future_value * 0.65,  # Assuming 65% equity allocation
            current_investments['equity'],
            years_to_goal,
            12,  # Expected equity returns
            equity_increase
        )
        
        required_debt = calculate_required_investment(
            future_value * 0.35,  # Assuming 35% debt allocation
            current_investments['debt'],
            years_to_goal,
            7,  # Expected debt returns
            debt_increase
        )
        
        # Display results
        st.subheader("Investment Summary")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            corpus_label = "Required Retirement Corpus" if is_retirement else "Future Value of Goal"
            st.metric(corpus_label, format_indian_currency(future_value))
            
        with col2:
            st.metric("Required Yearly Equity Investment", format_indian_currency(required_equity))
            
        with col3:
            st.metric("Required Yearly Debt Investment", format_indian_currency(required_debt))
        
        # Create and display projection plot
        st.subheader("Investment Projection")
        fig = create_investment_projection_plot(
            future_value if is_retirement else current_cost,
            inflation_rate,
            years_to_goal,
            current_investments['equity'],
            current_investments['debt'],
            required_equity,
            required_debt,
            equity_increase,
            debt_increase,
            is_retirement
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Display detailed plan
        st.subheader("Goal Planning Details")
        if is_retirement:
            plan_data = {
                'Parameter': [
                    'Goal Name',
                    'Current Annual Expenses',
                    'Current Age',
                    'Retirement Age',
                    'Life Expectancy',
                    'Years to Retirement',
                    'Inflation Rate',
                    'Current Equity Investment',
                    'Current Debt Investment',
                    'Yearly Equity Increase',
                    'Yearly Debt Increase',
                    'Required Yearly Equity Investment',
                    'Required Yearly Debt Investment',
                    'Required Retirement Corpus'
                ],
                'Value': [
                    selected_goal,
                    format_indian_currency(current_annual_expenses),
                    f"{current_age} years",
                    f"{retirement_age} years",
                    f"{life_expectancy} years",
                    f"{years_to_goal} years",
                    f"{inflation_rate}%",
                    format_indian_currency(current_investments['equity']),
                    format_indian_currency(current_investments['debt']),
                    f"{equity_increase}%",
                    f"{debt_increase}%",
                    format_indian_currency(required_equity),
                    format_indian_currency(required_debt),
                    format_indian_currency(future_value)
                ]
            }
        else:
            plan_data = {
                'Parameter': [
                    'Goal Name',
                    'Current Cost',
                    'Years to Goal',
                    'Inflation Rate',
                    'Current Equity Investment',
                    'Current Debt Investment',
                    'Yearly Equity Increase',
                    'Yearly Debt Increase',
                    'Required Yearly Equity Investment',
                    'Required Yearly Debt Investment',
                    'Expected Future Value'
                ],
                'Value': [
                    selected_goal,
                    format_indian_currency(current_cost),
                    f"{years_to_goal} years",
                    f"{inflation_rate}%",
                    format_indian_currency(current_investments['equity']),
                    format_indian_currency(current_investments['debt']),
                    f"{equity_increase}%",
                    f"{debt_increase}%",
                    format_indian_currency(required_equity),
                    format_indian_currency(required_debt),
                    format_indian_currency(future_value)
                ]
            }
        
        st.table(pd.DataFrame(plan_data))

if __name__ == "__main__":
    main()