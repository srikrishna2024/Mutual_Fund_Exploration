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

def calculate_total_growth(initial_equity, initial_debt, equity_rate, debt_rate, years, annual_investment, equity_allocation, debt_allocation, investment_increase=0):
    """Calculate cumulative growth including initial investments and yearly contributions with annual increase."""
    total_values = [initial_equity + initial_debt]
    current_annual_investment = annual_investment
    
    for year in range(1, years + 1):
        previous_total = total_values[-1]
        yearly_equity_contribution = current_annual_investment * (equity_allocation / 100)
        yearly_debt_contribution = current_annual_investment * (debt_allocation / 100)
        
        equity_growth = (previous_total * (equity_allocation / 100) + yearly_equity_contribution) * (1 + equity_rate)
        debt_growth = (previous_total * (debt_allocation / 100) + yearly_debt_contribution) * (1 + debt_rate)
        
        total_values.append(equity_growth + debt_growth)
        # Increase the investment for next year
        current_annual_investment *= (1 + investment_increase)
    
    return total_values, current_annual_investment

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
def calculate_retirement_needs(current_expenses, inflation_rate, retirement_years, life_expectancy):
    """Calculate total retirement corpus needed based on expenses and life expectancy."""
    years_in_retirement = life_expectancy - retirement_years
    future_annual_expense = current_expenses * (1 + inflation_rate) ** retirement_years
    total_corpus_needed = 0
    
    for year in range(years_in_retirement):
        expense_in_year = future_annual_expense * (1 + inflation_rate) ** year
        total_corpus_needed += expense_in_year
    
    return total_corpus_needed

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

    goals = get_goals()
    if not goals:
        st.warning("No goals found in the database.")
    else:
        selected_goal = st.selectbox("Select Goal", goals)
        if selected_goal:
            equity, debt = get_goal_data(selected_goal)
            st.write(f"Initial Equity: ₹{format_indian_number(equity)}, Initial Debt: ₹{format_indian_number(debt)}")

            # Common investment inputs for all goals
            st.subheader("Investment Details")
            col1, col2 = st.columns(2)
            with col1:
                annual_investment = st.number_input("Initial Yearly Investment (₹)", min_value=0, value=50000)
            with col2:
                investment_increase = st.number_input("Yearly Investment Increase (%)", min_value=0.0, max_value=50.0, value=5.0) / 100

            # Show projected investments table for all goals
            if investment_increase > 0:
                with st.expander("View Projected Yearly Investments"):
                    projected_investments = pd.DataFrame({
                        'Year': range(1, 31),  # Show up to 30 years
                        'Yearly Investment': [annual_investment * (1 + investment_increase) ** (year - 1) for year in range(1, 31)]
                    })
                    st.dataframe(
                        projected_investments.style.format({'Yearly Investment': '₹{:,.0f}'}),
                        height=200
                    )
                    total_investment = projected_investments['Yearly Investment'].sum()
                    st.write(f"Total projected investment over 30 years: ₹{format_indian_number(total_investment)}")

            if selected_goal.lower() == "retirement":
                # Retirement specific inputs
                st.subheader("Retirement Planning Details")
                col1, col2, col3 = st.columns(3)
                with col1:
                    current_age = st.number_input("Current Age", min_value=20, max_value=70, value=30)
                with col2:
                    retirement_age = st.number_input("Retirement Age", min_value=current_age + 1, max_value=80, value=60)
                with col3:
                    life_expectancy = st.number_input("Life Expectancy", min_value=retirement_age + 1, max_value=100, value=80)
                
                current_expenses = st.number_input("Current Annual Expenses (₹)", min_value=0, value=500000)
                years = retirement_age - current_age
                
                # Calculate retirement needs with escalating investments
                inflation_rate = st.number_input("Expected Inflation Rate (%)", min_value=0.0, value=5.0) / 100
                retirement_corpus_needed = calculate_retirement_needs(
                    current_expenses,
                    inflation_rate,
                    years,
                    life_expectancy
                )
                
                st.write(f"Required Retirement Corpus: ₹{format_indian_number(retirement_corpus_needed)}")
                
                risk_profile = st.selectbox(
                    "Select Your Risk Profile",
                    ["Conservative", "Moderate", "Aggressive"]
                )
                
                # Return rate inputs
                equity_rate = st.number_input("Expected Equity Return (%)", min_value=0.0, value=12.0) / 100
                debt_rate = st.number_input("Expected Debt Return (%)", min_value=0.0, value=7.0) / 100
                
                suggested_equity, suggested_debt, projected_value = suggest_retirement_allocation(
                        retirement_corpus_needed,
                        equity + debt,
                        years,
                        equity_rate,
                        debt_rate,
                        annual_investment,
                        investment_increase,  # Add this parameter
                        risk_profile
                )
                
                # Display retirement insights
                st.subheader("Retirement Planning Insights")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"Suggested Portfolio Split:")
                    st.write(f"- Equity: {suggested_equity:.1f}%")
                    st.write(f"- Debt: {suggested_debt:.1f}%")
                with col2:
                    shortfall = retirement_corpus_needed - projected_value
                    if shortfall > 0:
                        st.error(f"Projected Shortfall: ₹{format_indian_number(shortfall)}")
                        additional_monthly = (shortfall * (1 / (1 + equity_rate) ** years)) / (12 * years)
                        st.write(f"Suggested Additional Monthly Investment: ₹{format_indian_number(additional_monthly)}")
                    else:
                        st.success("You are on track for retirement!")
                
                simulation_plot = create_simulation_plot(
                    years,
                    equity + debt,
                    equity_rate,
                    debt_rate,
                    suggested_equity,
                    suggested_debt,
                    annual_investment
                )
                st.plotly_chart(simulation_plot)
                
            else:
                # Non-retirement goals
                col1, col2 = st.columns(2)
                with col1:
                    current_cost = st.number_input("Current Cost of the Goal (₹)", min_value=0, value=1000000)
                with col2:
                    years = st.slider("Years to Goal", 1, 30, 10)

                st.subheader("Inflation Considerations")
                inflation_rate = st.number_input("Expected Inflation Rate (%)", min_value=0.0, value=5.0) / 100
                inflation_adjusted_target = current_cost * (1 + inflation_rate) ** years
                st.write(f"Inflation-Adjusted Target Amount: ₹{format_indian_number(inflation_adjusted_target)}")

                if equity + debt >= inflation_adjusted_target:
                    st.success("Your current investments already exceed the inflation-adjusted target amount. No further action is required.")
                else:
                    st.subheader("Asset Allocation")
                    allocation = st.slider("Equity Allocation (%)", 0, 100, 60)
                    equity_allocation = allocation
                    debt_allocation = 100 - allocation
                    st.write(f"Equity: {equity_allocation}%, Debt: {debt_allocation}%")

                    st.subheader("Return Expectations")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        equity_rate = st.number_input("Expected Equity Return (%)", min_value=0.0, value=12.0) / 100
                    with col2:
                        debt_rate = st.number_input("Expected Debt Return (%)", min_value=0.0, value=7.0) / 100
                    with col3:
                        benchmark_rate = st.number_input("Expected Benchmark Return (%)", min_value=0.0, value=12.0) / 100

                    # Calculate growth with investment increase
                    total_growth, final_investment = calculate_total_growth(
                        equity, debt, equity_rate, debt_rate, years, annual_investment,
                        equity_allocation, debt_allocation, investment_increase
                    )
                    conservative_growth, _ = calculate_total_growth(
                        equity, debt, equity_rate * 0.9, debt_rate * 0.9, years,
                        annual_investment, equity_allocation, debt_allocation, investment_increase
                    )
                    
                    # Calculate benchmark growth with increasing investments
                    benchmark_values = [equity + debt]
                    current_investment = annual_investment
                    for year in range(1, years + 1):
                        new_value = benchmark_values[-1] * (1 + benchmark_rate) + current_investment
                        benchmark_values.append(new_value)
                        current_investment *= (1 + investment_increase)
                    benchmark_growth = benchmark_values

                    st.subheader("Insights")
                    if conservative_growth[-1] >= benchmark_growth[-1]:
                        st.success("You are on track to meet your goal!")
                    else:
                        st.error("You are off track. Consider adjusting your investments.")
                        suggested_equity, suggested_debt, suggested_investment = suggest_allocation_adjustment(
                            inflation_adjusted_target, equity + debt, equity_rate, debt_rate,
                            years, annual_investment, investment_increase
                        )
                        st.write(f"Suggested Allocation: Equity {suggested_equity}%, Debt {suggested_debt}%")
                        st.write(f"Suggested Initial Yearly Investment: ₹{format_indian_number(suggested_investment)}")

                        required_return = ((inflation_adjusted_target / (equity + debt)) ** (1 / years)) - 1
                        st.write(f"Required Return to Meet Goal: {required_return * 100:.2f}%")

                        simulation_plot = create_simulation_plot(
                            years, equity + debt, equity_rate, debt_rate,
                            suggested_equity, suggested_debt, suggested_investment
                        )
                        st.plotly_chart(simulation_plot)

                    st.subheader("Key Metrics")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Growth Value", f"₹{format_indian_number(total_growth[-1])}")
                    with col2:
                        st.metric("Conservative Final Value", f"₹{format_indian_number(conservative_growth[-1])}")
                    with col3:
                        st.metric("Benchmark Final Value", f"₹{format_indian_number(benchmark_growth[-1])}")

                    st.subheader("Investment Growth Comparison")
                    comparison_plot = create_comparison_plot(years, total_growth, conservative_growth, benchmark_growth)
                    st.plotly_chart(comparison_plot)

if __name__ == "__main__":
    main()