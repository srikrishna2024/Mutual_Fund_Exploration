import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime
import numpy as np

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

# Utility Functions
def format_indian_numbers(number):
    """Format numbers in Indian style (lakhs and crores)"""
    if number < 0:
        return f"-₹{format_indian_numbers(-number)[1:]}"
    
    s = str(int(number))
    if len(s) > 7:
        # Crores
        l = len(s)
        crore = s[0:l-7]
        left = s[l-7:]
        formatted = f"{','.join([crore[i:i+2] for i in range(0, len(crore), 2)]).lstrip(',')},{left[:2]},{left[2:4]},{left[4:]}"
    else:
        # Lakhs
        l = len(s)
        if l > 5:
            formatted = s[:-5] + "," + s[-5:-3] + "," + s[-3:]
        elif l > 3:
            formatted = s[:-3] + "," + s[-3:]
        else:
            formatted = s
    
    decimal_part = f"{number:.2f}".split('.')[1]
    return f"₹{formatted}.{decimal_part}"

@st.cache_resource
def init_connection():
    """Initialize database connection"""
    try:
        return psycopg.connect(**DB_PARAMS)
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

def calculate_future_value(principal, monthly_investment, rate, years, yearly_increase):
    """Calculate future value of investments with monthly additions and yearly increase"""
    future_value = principal * (1 + rate/100) ** years
    
    for year in range(years):
        yearly_investment = monthly_investment * 12 * (1 + yearly_increase/100) ** year
        future_value += yearly_investment * (1 + rate/100) ** (years - year)
    
    return future_value

def calculate_future_expenses(current_expenses, inflation, years):
    """Calculate future annual expenses considering inflation"""
    return current_expenses * (1 + inflation/100) ** years

def calculate_required_monthly_investment(target_amount, current_investment, rate, years, yearly_increase):
    """Calculate required monthly investment to reach target amount"""
    low = 0
    high = target_amount / (12 * years)  # Initial upper bound
    target_value = target_amount
    
    while high - low > 1:
        mid = (low + high) / 2
        future_value = calculate_future_value(current_investment, mid, rate, years, yearly_increase)
        
        if abs(future_value - target_value) < 1000:
            return mid
        elif future_value < target_value:
            low = mid
        else:
            high = mid
    
    return (low + high) / 2

def calculate_investment_growth(current_investment, monthly_investment, rate, years, yearly_increase):
    """Calculate yearly investment growth with monthly additions"""
    yearly_values = []
    current_value = current_investment
    
    for year in range(years + 1):
        yearly_values.append(current_value)
        
        if year < years:
            current_value *= (1 + rate/100)
            monthly_amt = monthly_investment * (1 + yearly_increase/100) ** year
            current_value += monthly_amt * 12
    
    return yearly_values

# Database Functions
def create_retirement_table():
    """Create retirement table if it doesn't exist"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS retirement (
                        id SERIAL PRIMARY KEY,
                        analysis_date DATE NOT NULL,
                        current_age INTEGER NOT NULL,
                        retirement_age INTEGER NOT NULL,
                        years_to_retirement INTEGER NOT NULL,
                        current_annual_expenses NUMERIC(20, 2) NOT NULL,
                        current_investments_equity NUMERIC(20, 2),
                        current_investments_debt NUMERIC(20, 2),
                        inflation NUMERIC(5, 2) NOT NULL,
                        equity_increase_yearly NUMERIC(5, 2),
                        debt_increase_yearly NUMERIC(5, 2),
                        equity_returns NUMERIC(5, 2),
                        debt_returns NUMERIC(5, 2),
                        required_retirement_corpus NUMERIC(20, 2),
                        required_monthly_equity NUMERIC(20, 2),
                        required_monthly_debt NUMERIC(20, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
        except Exception as e:
            st.error(f"Failed to create retirement table: {str(e)}")

def create_goal_planner_table():
    """Create goal_planner table if it doesn't exist"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS goal_planner (
                        id SERIAL PRIMARY KEY,
                        age INTEGER NOT NULL,
                        goal_name VARCHAR(255) NOT NULL,
                        years_to_goal INTEGER NOT NULL,
                        current_cost NUMERIC(20, 2) NOT NULL,
                        inflation NUMERIC(5, 2) NOT NULL,
                        existing_equity NUMERIC(20, 2),
                        existing_debt NUMERIC(20, 2),
                        estimated_returns_debt NUMERIC(5, 2),
                        estimated_returns_equity NUMERIC(5, 2),
                        equity_increase_yearly NUMERIC(5, 2),
                        debt_increase_yearly NUMERIC(5, 2),
                        monthly_investment_equity NUMERIC(20, 2),
                        monthly_investment_debt NUMERIC(20, 2),
                        future_cost NUMERIC(20, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                conn.commit()
        except Exception as e:
            st.error(f"Failed to create goal planner table: {str(e)}")

def insert_retirement_data(data):
    """Insert retirement analysis data into the database"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO retirement (
                        analysis_date, current_age, retirement_age, years_to_retirement,
                        current_annual_expenses, current_investments_equity, current_investments_debt,
                        inflation, equity_increase_yearly, debt_increase_yearly,
                        equity_returns, debt_returns, required_retirement_corpus,
                        required_monthly_equity, required_monthly_debt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data)
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Failed to insert retirement data: {str(e)}")
            return False

def insert_goal_data(goal_data):
    """Insert goal planning data into the database"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO goal_planner (
                        age, goal_name, years_to_goal, current_cost, inflation,
                        existing_equity, existing_debt, estimated_returns_debt,
                        estimated_returns_equity, equity_increase_yearly,
                        debt_increase_yearly, monthly_investment_equity,
                        monthly_investment_debt, future_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, goal_data)
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Failed to insert goal data: {str(e)}")
            return False

def get_retirement_data():
    """Retrieve all retirement analysis data from the database"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT 
                    analysis_date, current_age, retirement_age, years_to_retirement,
                    current_annual_expenses, current_investments_equity, current_investments_debt,
                    inflation, equity_increase_yearly, debt_increase_yearly,
                    equity_returns, debt_returns, required_retirement_corpus,
                    required_monthly_equity, required_monthly_debt,
                    created_at
                FROM retirement 
                ORDER BY created_at DESC
            """
            df = pd.read_sql_query(query, conn)
            
            currency_columns = [
                'current_annual_expenses', 'current_investments_equity', 
                'current_investments_debt', 'required_retirement_corpus',
                'required_monthly_equity', 'required_monthly_debt'
            ]
            
            for col in currency_columns:
                df[col] = df[col].apply(format_indian_numbers)
            
            return df
        except Exception as e:
            st.error(f"Failed to retrieve retirement data: {str(e)}")
            return pd.DataFrame()

def get_goal_data():
    """Retrieve all goal planning data from the database"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT age, goal_name, years_to_goal, current_cost, inflation,
                       existing_equity, existing_debt, estimated_returns_debt,
                       estimated_returns_equity, equity_increase_yearly,
                       debt_increase_yearly, monthly_investment_equity,
                       monthly_investment_debt, future_cost
                FROM goal_planner 
                ORDER BY created_at DESC
            """
            df = pd.read_sql_query(query, conn)
            
            currency_columns = [
                'current_cost', 'existing_equity', 'existing_debt',
                'monthly_investment_equity', 'monthly_investment_debt', 'future_cost'
            ]
            
            for col in currency_columns:
                df[col] = df[col].apply(format_indian_numbers)
            
            return df
        except Exception as e:
            st.error(f"Failed to retrieve goal data: {str(e)}")
            return pd.DataFrame()

def retirement_planning_tab():
    """Retirement Planning Tab Content"""
    st.title("Retirement Planning Calculator")
    
    create_retirement_table()
    
    with st.form("retirement_planner_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            current_age = st.number_input("Current Age", min_value=18, max_value=100)
            retirement_age = st.number_input("Desired Retirement Age", min_value=current_age + 1, max_value=100)
            years_to_retirement = int(retirement_age - current_age)
            st.info(f"Years to Retirement: {years_to_retirement}")
            current_annual_expenses = st.number_input("Current Annual Expenses", min_value=0.0)
            inflation = st.number_input("Expected Inflation (%)", min_value=0.0, max_value=20.0, value=6.0)
        
        with col2:
            current_investments_equity = st.number_input("Current Investments - Equity", min_value=0.0)
            current_investments_debt = st.number_input("Current Investments - Debt", min_value=0.0)
            equity_returns = st.number_input("Expected Equity Returns (%)", min_value=0.0, max_value=30.0, value=12.0)
            debt_returns = st.number_input("Expected Debt Returns (%)", min_value=0.0, max_value=20.0, value=7.0)
        
        with col3:
            equity_increase_yearly = st.number_input("Yearly Increase in Equity Investments (%)", min_value=0.0, max_value=50.0, value=10.0)
            debt_increase_yearly = st.number_input("Yearly Increase in Debt Investments (%)", min_value=0.0, max_value=50.0, value=5.0)
        
        submitted = st.form_submit_button("Calculate Retirement Plan")
        
        if submitted:
            # Perform retirement calculations
            future_annual_expenses = calculate_future_expenses(current_annual_expenses, inflation, years_to_retirement)
            weighted_return = (equity_returns * 0.6 + debt_returns * 0.4)
            required_corpus = (future_annual_expenses * 100) / max(4, weighted_return - 2)
            
            equity_portion = required_corpus * 0.6
            debt_portion = required_corpus * 0.4
            
            # Calculate required monthly investments
            required_monthly_equity = calculate_required_monthly_investment(
                equity_portion,
                current_investments_equity,
                equity_returns,
                years_to_retirement,
                equity_increase_yearly
            )
            
            required_monthly_debt = calculate_required_monthly_investment(
                debt_portion,
                current_investments_debt,
                debt_returns,
                years_to_retirement,
                debt_increase_yearly
            )
            
            # Calculate investment growth
            equity_growth = calculate_investment_growth(
                current_investments_equity,
                required_monthly_equity,
                equity_returns,
                years_to_retirement,
                equity_increase_yearly
            )
            
            debt_growth = calculate_investment_growth(
                current_investments_debt,
                required_monthly_debt,
                debt_returns,
                years_to_retirement,
                debt_increase_yearly
            )
            
            # Create growth chart data
            years = list(range(current_age, retirement_age + 1))
            chart_data = pd.DataFrame({
                'Age': years,
                'Equity': equity_growth,
                'Debt': debt_growth,
                'Total': [e + d for e, d in zip(equity_growth, debt_growth)]
            })
            
            # Display results
            st.subheader("Retirement Analysis")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Required Retirement Corpus", format_indian_numbers(required_corpus))
                st.metric("Future Annual Expenses", format_indian_numbers(future_annual_expenses))
            
            with col2:
                st.metric("Required Monthly Equity Investment", format_indian_numbers(required_monthly_equity))
            
            with col3:
                st.metric("Required Monthly Debt Investment", format_indian_numbers(required_monthly_debt))
            
            # Display investment growth chart
            st.subheader("Investment Growth Projection")
            st.line_chart(chart_data.set_index('Age'))
            
            # Save retirement data
            retirement_data = (
                datetime.today().date(), current_age, retirement_age, years_to_retirement,
                current_annual_expenses, current_investments_equity, current_investments_debt,
                inflation, equity_increase_yearly, debt_increase_yearly,
                equity_returns, debt_returns, required_corpus,
                required_monthly_equity, required_monthly_debt
            )
            
            if insert_retirement_data(retirement_data):
                st.success("Retirement plan saved successfully!")
    
    # Display saved retirement plans
    st.subheader("Previous Retirement Plans")
    retirement_data = get_retirement_data()
    if not retirement_data.empty:
        st.dataframe(retirement_data)
    else:
        st.info("No retirement plans saved yet. Please create a new plan using the form above.")

def goal_planning_tab():
    """Goal Planning Tab Content"""
    st.title("Financial Goal Planner")
    
    create_goal_planner_table()
    
    with st.form("goal_planner_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            age = st.number_input("Age of User", min_value=18, max_value=100)
            goal_name = st.text_input("Goal Name")
            years_to_goal = st.number_input("Years to Goal", min_value=1, max_value=50)
            current_cost = st.number_input("Current Cost of the Goal", min_value=0.0)
            inflation = st.number_input("Expected Inflation (%)", min_value=0.0, max_value=20.0, value=6.0)
        
        with col2:
            existing_equity = st.number_input("Existing Investments - Equity", min_value=0.0)
            existing_debt = st.number_input("Existing Investments - Debt", min_value=0.0)
            estimated_returns_debt = st.number_input("Estimated Returns - Debt (%)", min_value=0.0, max_value=20.0, value=7.0)
            estimated_returns_equity = st.number_input("Estimated Returns - Equity (%)", min_value=0.0, max_value=30.0, value=12.0)
        
        with col3:
            equity_increase_yearly = st.number_input("Yearly Increase in Equity Investments (%)", min_value=0.0, max_value=50.0, value=10.0)
            debt_increase_yearly = st.number_input("Yearly Increase in Debt Investments (%)", min_value=0.0, max_value=50.0, value=5.0)
            monthly_investment_equity = st.number_input("Current Monthly Investments - Equity", min_value=0.0)
            monthly_investment_debt = st.number_input("Current Monthly Investments - Debt", min_value=0.0)
        
        submitted = st.form_submit_button("Calculate and Save")
        
        if submitted:
            if not goal_name:
                st.error("Goal Name is required.")
                return
            
            # Calculate future cost of the goal
            future_cost = calculate_future_expenses(current_cost, inflation, years_to_goal)
            
            # Calculate future value of investments
            future_equity = calculate_future_value(
                existing_equity,
                monthly_investment_equity,
                estimated_returns_equity,
                years_to_goal,
                equity_increase_yearly
            )
            
            future_debt = calculate_future_value(
                existing_debt,
                monthly_investment_debt,
                estimated_returns_debt,
                years_to_goal,
                debt_increase_yearly
            )
            
            total_future_value = future_equity + future_debt
            
            # Calculate yearly projections for the chart
            equity_growth = calculate_investment_growth(
                existing_equity,
                monthly_investment_equity,
                estimated_returns_equity,
                years_to_goal,
                equity_increase_yearly
            )
            
            debt_growth = calculate_investment_growth(
                existing_debt,
                monthly_investment_debt,
                estimated_returns_debt,
                years_to_goal,
                debt_increase_yearly
            )
            
            # Create growth chart data
            years = list(range(age, age + years_to_goal + 1))
            chart_data = pd.DataFrame({
                'Age': years,
                'Equity': equity_growth,
                'Debt': debt_growth,
                'Total': [e + d for e, d in zip(equity_growth, debt_growth)]
            })
            
            # Display results
            st.subheader("Goal Analysis")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Future Cost of Goal", format_indian_numbers(future_cost))
            with col2:
                st.metric("Expected Future Value", format_indian_numbers(total_future_value))
            with col3:
                surplus_deficit = total_future_value - future_cost
                st.metric(
                    "Surplus/Deficit",
                    format_indian_numbers(abs(surplus_deficit)),
                    delta=f"{'Surplus' if surplus_deficit >= 0 else 'Deficit'}"
                )
            
            # Display investment growth chart
            st.subheader("Investment Growth Projection")
            st.line_chart(chart_data.set_index('Age'))
            
            # Save goal data
            goal_data = (
                age, goal_name, years_to_goal, current_cost, inflation,
                existing_equity, existing_debt, estimated_returns_debt,
                estimated_returns_equity, equity_increase_yearly,
                debt_increase_yearly, monthly_investment_equity,
                monthly_investment_debt, future_cost
            )
            
            if insert_goal_data(goal_data):
                st.success("Goal data saved successfully!")
    
    # Display existing goals
    st.subheader("Saved Goals")
    goals_data = get_goal_data()
    if not goals_data.empty:
        st.dataframe(goals_data)
    else:
        st.info("No goals saved yet. Please add a new goal using the form above.")

def main():
    """Main function to run the application"""
    st.set_page_config(
        page_title="Financial Planner",
        page_icon="💰",
        layout="wide"
    )
    
    # Create tabs
    tab1, tab2 = st.tabs(["Retirement Planning", "Goal Planning"])
    
    # Retirement Planning Tab
    with tab1:
        retirement_planning_tab()
    
    # Goal Planning Tab
    with tab2:
        goal_planning_tab()

if __name__ == "__main__":
    main()