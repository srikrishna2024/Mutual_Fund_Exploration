import streamlit as st
import pandas as pd
import psycopg
from plotly import graph_objects as go

def format_indian_number(number):
    """Format a number in Indian style with commas (e.g., 1,00,000)"""
    str_number = str(int(number))
    if len(str_number) <= 3:
        return str_number
    
    # Split the number into integer and decimal parts if it's a float
    if isinstance(number, float):
        decimal_part = f"{number:.2f}".split('.')[1]
    else:
        decimal_part = None
    
    # Format integer part with Indian style commas
    last_three = str_number[-3:]
    other_numbers = str_number[:-3]
    
    if other_numbers:
        formatted_number = ''
        for i, digit in enumerate(reversed(other_numbers)):
            if i % 2 == 0 and i != 0:
                formatted_number = ',' + formatted_number
            formatted_number = digit + formatted_number
        formatted_number = formatted_number + ',' + last_three
    else:
        formatted_number = last_three
    
    # Add decimal part if exists
    if decimal_part:
        formatted_number = f"{formatted_number}.{decimal_part}"
    
    return formatted_number

def connect_to_db():
    """Establish a database connection."""
    DB_PARAMS = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'admin123',
        'host': 'localhost',
        'port': '5432'
    }
    return psycopg.connect(**DB_PARAMS)

def get_goals():
    """Retrieve distinct goals from the goals table."""
    with connect_to_db() as conn:
        query = "SELECT DISTINCT goal_name FROM goals ORDER BY goal_name"
        return pd.read_sql(query, conn)['goal_name'].tolist()

def get_goal_data(goal_name):
    """Retrieve current equity and debt investment data for a selected goal."""
    with connect_to_db() as conn:
        query = """
        SELECT investment_type, SUM(current_value) AS total_value
        FROM goals
        WHERE goal_name = %s
        GROUP BY investment_type
        """
        df = pd.read_sql(query, conn, params=[goal_name])
        investments = {row['investment_type']: row['total_value'] for _, row in df.iterrows()}
        return investments.get('Equity', 0), investments.get('Debt', 0)

def calculate_growth(initial, rate, years, annual_contribution=0):
    """Calculate yearly growth based on compound interest and contributions."""
    values = [initial]
    for year in range(1, years + 1):
        new_value = values[-1] * (1 + rate) + annual_contribution
        values.append(new_value)
    return values

def calculate_total_growth_glidepath(
    initial_equity, initial_debt, equity_rate, debt_rate, years, annual_investment,
    starting_equity_allocation, ending_equity_allocation, allocation_change_start_year, investment_increase=0
):
    """
    Calculate yearly growth with a glide path for changing equity-debt allocation
    and sequence of returns risk.

    Returns:
    - A DataFrame with separate equity and debt growth lines.
    - Final total values.
    """
    # Initialize values
    equity_values = [initial_equity]
    debt_values = [initial_debt]
    total_values = [initial_equity + initial_debt]
    current_annual_investment = annual_investment

    # Calculate yearly allocation adjustments
    allocation_change_years = max(1, allocation_change_start_year)
    years_to_adjust = years - allocation_change_years + 1
    equity_allocations = [
        max(
            starting_equity_allocation - ((starting_equity_allocation - ending_equity_allocation) / years_to_adjust) * max(0, year - allocation_change_years),
            ending_equity_allocation
        )
        for year in range(years)
    ]
    debt_allocations = [100 - equity_allocation for equity_allocation in equity_allocations]

    # Yearly calculations
    for year in range(1, years + 1):
        previous_equity = equity_values[-1]
        previous_debt = debt_values[-1]

        # Current allocation percentages
        equity_allocation = equity_allocations[year - 1]
        debt_allocation = debt_allocations[year - 1]

        # Contributions based on allocation
        yearly_equity_contribution = current_annual_investment * (equity_allocation / 100)
        yearly_debt_contribution = current_annual_investment * (debt_allocation / 100)

        # Growth calculations
        equity_growth = (previous_equity + yearly_equity_contribution) * (1 + equity_rate)
        debt_growth = (previous_debt + yearly_debt_contribution) * (1 + debt_rate)

        # Append new values
        equity_values.append(equity_growth)
        debt_values.append(debt_growth)
        total_values.append(equity_growth + debt_growth)

        # Increase annual investment
        current_annual_investment *= (1 + investment_increase)

    # Create DataFrame for plotting
    growth_df = pd.DataFrame({
        'Year': list(range(1, years + 2)),
        'Equity': equity_values,
        'Debt': debt_values,
        'Total': total_values
    })

    return growth_df, total_values[-1]


def create_comparison_plot(years, expected_growth, conservative_growth, benchmark_growth):
    """Generate an interactive plot comparing different growth paths."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=expected_growth,
        name="Expected Growth",
        mode="lines+markers",
        hovertemplate="₹%{y:,.2f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        y=conservative_growth,
        name="Conservative Growth",
        mode="lines+markers",
        hovertemplate="₹%{y:,.2f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        y=benchmark_growth,
        name="Benchmark Growth",
        mode="lines+markers",
        hovertemplate="₹%{y:,.2f}<extra></extra>"
    ))
    fig.update_layout(
        title="Investment Growth Comparison",
        xaxis_title="Years",
        yaxis_title="Value (₹)",
        legend_title="Growth Paths",
        yaxis=dict(
            tickformat=",",
            separatethousands=True
        )
    )
    return fig

def suggest_allocation_adjustment(target, actual, equity_rate, debt_rate, years, annual_investment, investment_increase=0):
    """Suggest an optimal equity-debt allocation to meet the target."""
    for equity_split in range(100, -1, -1):
        debt_split = 100 - equity_split
        growth_values, _ = calculate_total_growth(
            actual, 0, equity_rate, debt_rate, years, 
            annual_investment, equity_split, debt_split, 
            investment_increase
        )
        if growth_values[-1] >= target:
            return equity_split, debt_split, annual_investment

    # If no solution found, try increasing the investment amount
    for increment in range(1, 101):
        increased_investment = annual_investment * (1 + increment / 100)
        for equity_split in range(100, -1, -1):
            debt_split = 100 - equity_split
            growth_values, _ = calculate_total_growth(
                actual, 0, equity_rate, debt_rate, years,
                increased_investment, equity_split, debt_split,
                investment_increase
            )
            if growth_values[-1] >= target:
                return equity_split, debt_split, increased_investment
    return 100, 0, annual_investment

def create_simulation_plot(years, initial, equity_rate, debt_rate, equity_split, debt_split, investment):
    """Create a simulation plot for suggested allocation."""
    projected_growth = calculate_growth(
        initial,
        (equity_rate * equity_split / 100 + debt_rate * debt_split / 100),
        years,
        investment
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=projected_growth,
        name="Projected Growth",
        mode="lines+markers",
        hovertemplate="₹%{y:,.2f}<extra></extra>"
    ))
    fig.update_layout(
        title="Simulation of Suggested Allocation",
        xaxis_title="Years",
        yaxis_title="Value (₹)",
        legend_title="Simulation",
        yaxis=dict(
            tickformat=",",
            separatethousands=True
        )
    )
    return fig
def create_simulation_plot_with_glidepath(growth_df):
    """
    Create a simulation plot showing equity, debt, and total growth over the years.
    """
    fig = go.Figure()

    # Add equity growth line
    fig.add_trace(go.Scatter(
        x=growth_df['Year'],
        y=growth_df['Equity'],
        mode='lines+markers',
        name='Equity Growth',
        hovertemplate="Year %{x}<br>₹%{y:,.2f}<extra></extra>"
    ))

    # Add debt growth line
    fig.add_trace(go.Scatter(
        x=growth_df['Year'],
        y=growth_df['Debt'],
        mode='lines+markers',
        name='Debt Growth',
        hovertemplate="Year %{x}<br>₹%{y:,.2f}<extra></extra>"
    ))

    # Add total growth line
    fig.add_trace(go.Scatter(
        x=growth_df['Year'],
        y=growth_df['Total'],
        mode='lines+markers',
        name='Total Growth',
        hovertemplate="Year %{x}<br>₹%{y:,.2f}<extra></extra>"
    ))

    # Configure plot layout
    fig.update_layout(
        title="Investment Growth with Glide Path",
        xaxis_title="Year",
        yaxis_title="Value (₹)",
        legend_title="Growth Paths",
        yaxis=dict(tickformat=",", separatethousands=True)
    )

    return fig

def calculate_retirement_needs(current_expenses, inflation_rate, years_to_retire, life_expectancy):
    """
    Calculate detailed retirement corpus needs with year-by-year breakdown.
    Returns both total corpus needed and yearly expenses dataframe.
    """
    retirement_age = 60  # Retirement starts at age 60
    retirement_years = life_expectancy - retirement_age  # Years in retirement
    future_annual_expense = current_expenses * (1 + inflation_rate) ** years_to_retire

    # Create lists for year-by-year breakdown
    years_list = []
    age_list = []
    expenses_list = []
    cumulative_corpus_list = []

    total_corpus_needed = 0  # Total retirement corpus
    for year in range(retirement_years):
        # Expense for each year during retirement
        expense_in_year = future_annual_expense * (1 + inflation_rate) ** year
        years_list.append(year + 1)
        age_list.append(retirement_age + year)
        expenses_list.append(expense_in_year)

        # Calculate remaining corpus needed for future years
        remaining_years = retirement_years - year
        year_corpus = sum(
            expense_in_year / ((1 + inflation_rate) ** future_year)
            for future_year in range(remaining_years)
        )
        cumulative_corpus_list.append(year_corpus)
        if year == 0:  # The total corpus needed at retirement age
            total_corpus_needed = year_corpus

    # Create DataFrame with the breakdown
    breakdown_df = pd.DataFrame({
        'Year': years_list,
        'Age': age_list,
        'Annual Expenses': expenses_list,
        'Corpus Required': cumulative_corpus_list
    })

    return total_corpus_needed, breakdown_df

def suggest_retirement_allocation(target_corpus, current_corpus, years_to_retire, equity_rate, debt_rate, annual_investment, investment_increase, risk_profile='Moderate'):
    """
    Suggest retirement portfolio allocation based on years to retirement and risk profile.
    Now includes investment_increase parameter.
    """
    if years_to_retire > 20:
        base_equity = 75
    elif years_to_retire > 10:
        base_equity = 65
    elif years_to_retire > 5:
        base_equity = 50
    else:
        base_equity = 40
    
    risk_adjustments = {
        'Conservative': -10,
        'Moderate': 0,
        'Aggressive': 10
    }
    
    equity_allocation = min(80, max(20, base_equity + risk_adjustments.get(risk_profile, 0)))
    debt_allocation = 100 - equity_allocation
    
    # Calculate with investment increase
    projected_values, _ = calculate_total_growth(
        current_corpus * (equity_allocation/100),
        current_corpus * (debt_allocation/100),
        equity_rate,
        debt_rate,
        years_to_retire,
        annual_investment,
        equity_allocation,
        debt_allocation,
        investment_increase
    )
    
    return equity_allocation, debt_allocation, projected_values[-1]

def main():
    st.set_page_config(page_title="Are We On Track Tool", layout="wide")
    st.title("Are We On Track Tool")

    # Retrieve goals from the database
    goals = get_goals()
    if not goals:
        st.warning("No goals found in the database.")
        return

    # Goal selection
    selected_goal = st.selectbox("Select Goal", goals)
    if not selected_goal:
        return

    equity, debt = get_goal_data(selected_goal)
    st.write(f"Initial Equity: ₹{format_indian_number(equity)}, Initial Debt: ₹{format_indian_number(debt)}")

    # Investment details
    st.subheader("Investment Details")
    col1, col2 = st.columns(2)
    with col1:
        annual_investment = st.number_input("Initial Yearly Investment (₹)", min_value=0, value=50000)
    with col2:
        investment_increase = st.number_input("Yearly Investment Increase (%)", min_value=0.0, max_value=50.0, value=5.0) / 100

    years = st.slider("Years to Goal", min_value=1, max_value=30, value=10)

    # Target Corpus for Non-Retirement Goals
    target_corpus = None
    if selected_goal.lower() != "retirement":
        target_corpus = st.number_input("Target Corpus (₹)", min_value=0, value=1000000)

    # Glide path allocation details
    st.subheader("Glide Path Allocation Details")
    if selected_goal.lower() == "retirement":
        col1, col2 = st.columns(2)
        with col1:
            starting_equity_allocation = st.slider(
                "Starting Equity Allocation (%)", min_value=50, max_value=100, value=60
            )
            ending_equity_allocation = st.slider(
                "Ending Equity Allocation (%)", min_value=0, max_value=50, value=40
            )
        with col2:
            years_before_retirement_to_start = st.slider(
                "Years Before Retirement to Start Adjusting Allocation", min_value=1, max_value=years - 1, value=5
            )
    else:
        col1, col2 = st.columns(2)
        with col1:
            starting_equity_allocation = st.slider(
                "Equity Allocation (%)", min_value=0, max_value=100, value=60
            )
        with col2:
            ending_equity_allocation = 100 - starting_equity_allocation
            st.write(f"Debt Allocation: {ending_equity_allocation}%")

    # Return expectations
    st.subheader("Return Expectations")
    col1, col2, col3 = st.columns(3)
    with col1:
        equity_rate = st.number_input("Expected Equity Return (%)", min_value=0.0, value=12.0) / 100
    with col2:
        debt_rate = st.number_input("Expected Debt Return (%)", min_value=0.0, value=7.0) / 100
    with col3:
        benchmark_rate = st.number_input("Expected Benchmark Return (%)", min_value=0.0, value=12.0) / 100

    # Calculate growth with glide path
    allocation_change_start_year = years - years_before_retirement_to_start if selected_goal.lower() == "retirement" else years
    growth_df, final_value = calculate_total_growth_glidepath(
        equity, debt, equity_rate, debt_rate, years, annual_investment,
        starting_equity_allocation, ending_equity_allocation,
        allocation_change_start_year, investment_increase
    )

    # Calculate benchmark growth
    benchmark_values = [equity + debt]
    current_investment = annual_investment
    for year in range(1, years + 1):
        new_value = benchmark_values[-1] * (1 + benchmark_rate) + current_investment
        benchmark_values.append(new_value)
        current_investment *= (1 + investment_increase)

    # Display the simulation plot
    st.subheader("Simulation Plot")
    simulation_plot = create_simulation_plot_with_glidepath(growth_df)
    simulation_plot.add_trace(go.Scatter(
        x=list(range(1, years + 2)),
        y=benchmark_values,
        mode='lines+markers',
        name='Benchmark Growth',
        hovertemplate="Year %{x}<br>₹%{y:,.2f}<extra></extra>"
    ))
    st.plotly_chart(simulation_plot)

    # Insights
    st.subheader("Insights")
    final_equity = growth_df['Equity'].iloc[-1]
    final_debt = growth_df['Debt'].iloc[-1]
    st.write(f"Final Equity: ₹{format_indian_number(final_equity)}")
    st.write(f"Final Debt: ₹{format_indian_number(final_debt)}")
    st.write(f"Total Final Value: ₹{format_indian_number(final_value)}")

    benchmark_final = benchmark_values[-1]

    # On Track or Off Track Analysis
    if selected_goal.lower() == "retirement":
        if final_value >= benchmark_final:
            st.success("You are on track to meet your goal!")
        else:
            st.error("You are off track. Consider adjusting your investments.")
    else:
        if equity + debt >= target_corpus:
            st.success("You are already on track to meet your target corpus!")
        elif final_value >= target_corpus:
            st.success("You are on track to meet your target corpus!")
        else:
            st.error("You are off track. Consider increasing your investments or adjusting your allocation.")

    if selected_goal.lower() == "retirement":
        st.subheader("Retirement Planning Insights")
        current_age = st.number_input("Current Age", min_value=20, max_value=70, value=30)
        retirement_age = st.number_input("Retirement Age", min_value=current_age + 1, max_value=80, value=60)
        life_expectancy = st.number_input("Life Expectancy", min_value=retirement_age + 1, max_value=100, value=80)
        current_expenses = st.number_input("Current Annual Expenses (₹)", min_value=0, value=500000)
        inflation_rate = st.number_input("Expected Inflation Rate (%)", min_value=0.0, value=5.0) / 100

        years_to_retire = retirement_age - current_age
        retirement_corpus_needed, retirement_breakdown = calculate_retirement_needs(
            current_expenses, inflation_rate, years_to_retire, life_expectancy
        )

        st.write(f"Total Retirement Corpus Required at Age {retirement_age}: ₹{format_indian_number(retirement_corpus_needed)}")

        with st.expander("View Year-by-Year Retirement Breakdown"):
            st.dataframe(retirement_breakdown.style.format(
                {"Annual Expenses": "₹{:,.2f}", "Corpus Required": "₹{:,.2f}"}
            ))

if __name__ == "__main__":
    main()
