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

# Initialize database connection
@st.cache_resource
def init_connection():
    try:
        return psycopg.connect(**DB_PARAMS)
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

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
            st.error(f"Failed to create table: {str(e)}")

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
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, goal_data)
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Failed to insert data: {str(e)}")
            return False

def get_goal_data():
    """Retrieve all records from goal_planner table"""
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
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Failed to retrieve data: {str(e)}")
            return pd.DataFrame()

def calculate_future_cost(current_cost, inflation, years):
    """Calculate future cost considering inflation"""
    return current_cost * (1 + inflation/100) ** years

def calculate_future_value(principal, monthly_investment, rate, years, yearly_increase):
    """Calculate future value of investments with monthly additions and yearly increase"""
    future_value = principal * (1 + rate/100) ** years
    
    for year in range(years):
        yearly_investment = monthly_investment * 12 * (1 + yearly_increase/100) ** year
        future_value += yearly_investment * (1 + rate/100) ** (years - year)
    
    return future_value

def main():
    st.set_page_config(
        page_title="Goal Planner",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("Financial Goal Planner")
    
    # Create database table if it doesn't exist
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
            future_cost = calculate_future_cost(current_cost, inflation, years_to_goal)
            
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
            
            # Display results
            st.subheader("Goal Analysis")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Future Cost of Goal", f"₹{future_cost:,.2f}")
            with col2:
                st.metric("Expected Future Value", f"₹{total_future_value:,.2f}")
            with col3:
                surplus_deficit = total_future_value - future_cost
                st.metric(
                    "Surplus/Deficit",
                    f"₹{abs(surplus_deficit):,.2f}",
                    delta=f"{'Surplus' if surplus_deficit >= 0 else 'Deficit'}"
                )
            
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

if __name__ == "__main__":
    main()
