import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime

def format_indian_currency(amount):
    """Format amount in Indian currency style (lakhs, crores)"""
    def format_number(num):
        if num < 0:
            return f"-{format_number(abs(num))}"
        if num < 1000:
            return str(num)
        elif num < 100000:
            return f"{num/1000:.2f}K"
        elif num < 10000000:
            return f"{num/100000:.2f}L"
        else:
            return f"{num/10000000:.2f}Cr"
    
    return f"₹{format_number(float(amount))}"

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

def check_and_update_schema():
    """Check if goals table exists and has required columns, update if necessary"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            # First, check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'goals'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table with all required columns
                cur.execute("""
                    CREATE TABLE goals (
                        id SERIAL PRIMARY KEY,
                        goal_name VARCHAR(100),
                        investment_type VARCHAR(20),
                        scheme_name VARCHAR(200),
                        scheme_code VARCHAR(50),
                        current_value DECIMAL(15,2),
                        is_manual_entry BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                # Check if is_manual_entry column exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = 'goals' AND column_name = 'is_manual_entry'
                    );
                """)
                has_manual_entry = cur.fetchone()[0]
                
                if not has_manual_entry:
                    # Add is_manual_entry column
                    cur.execute("""
                        ALTER TABLE goals 
                        ADD COLUMN is_manual_entry BOOLEAN DEFAULT FALSE
                    """)
            
        conn.commit()

def get_portfolio_data():
    """Retrieve current portfolio data with latest NAVs"""
    with connect_to_db() as conn:
        query = """
            WITH latest_units AS (
                SELECT scheme_name, code, SUM(
                    CASE 
                        WHEN transaction_type = 'switch' THEN -units
                        WHEN transaction_type = 'redeem' THEN -units
                        ELSE units 
                    END
                ) as total_units
                FROM portfolio_data
                GROUP BY scheme_name, code
                HAVING SUM(
                    CASE 
                        WHEN transaction_type = 'switch' THEN -units
                        WHEN transaction_type = 'redeem' THEN -units
                        ELSE units 
                    END
                ) > 0
            ),
            latest_nav AS (
                SELECT code, value as nav_value
                FROM mutual_fund_nav
                WHERE (code, nav) IN (
                    SELECT code, MAX(nav)
                    FROM mutual_fund_nav
                    GROUP BY code
                )
            )
            SELECT 
                lu.scheme_name,
                lu.code as scheme_code,
                lu.total_units * ln.nav_value as current_value
            FROM latest_units lu
            JOIN latest_nav ln ON lu.code = ln.code
        """
        return pd.read_sql(query, conn)

def check_existing_mapping(scheme_name, scheme_code):
    """Check if a fund is already mapped to any goal"""
    with connect_to_db() as conn:
        query = """
            SELECT goal_name 
            FROM goals 
            WHERE scheme_name = %s AND scheme_code = %s
            AND (is_manual_entry IS NULL OR is_manual_entry = FALSE)
        """
        df = pd.read_sql(query, conn, params=(scheme_name, scheme_code))
        return df['goal_name'].iloc[0] if not df.empty else None

def insert_goal_mapping(goal_name, investment_type, scheme_name, scheme_code, current_value, is_manual_entry=False):
    """Insert a new goal mapping into the goals table"""
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO goals 
                    (goal_name, investment_type, scheme_name, scheme_code, current_value, is_manual_entry)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (goal_name, investment_type, scheme_name, scheme_code, current_value, is_manual_entry))
                
                inserted_id = cur.fetchone()[0]
                conn.commit()
                return True
    except Exception as e:
        print(f"Error inserting record: {str(e)}")
        return False

def get_existing_goals():
    """Retrieve existing goal mappings"""
    with connect_to_db() as conn:
        query = """
            SELECT 
                goal_name, 
                investment_type, 
                scheme_name, 
                scheme_code, 
                current_value,
                COALESCE(is_manual_entry, FALSE) as is_manual_entry
            FROM goals
            ORDER BY goal_name, scheme_name
        """
        return pd.read_sql(query, conn)

def main():
    """
    Main function to run the Investment Goal Mapping Tool application.
    This function sets up the Streamlit page configuration, displays the title,
    and initializes the application by checking and updating the database schema.
    It then retrieves the current portfolio data and creates two tabs for mutual
    fund mapping and manual investment entry. Users can map mutual funds to goals
    or add manual investments through forms. The function also displays existing
    goal mappings and provides a summary of the total portfolio value.
    Tabs:
        - Mutual Fund Mapping: Allows users to map mutual funds to specific goals.
        - Manual Investment Entry: Allows users to add manual investments with details.
    Forms:
        - goal_mapping_form: Form to map mutual funds to goals.
        - manual_investment_form: Form to add manual investments.
    Displays:
        - Goal-wise summary of current goal mappings.
        - Total portfolio value.
        - Detailed mappings of existing goals.
    Raises:
        Streamlit warnings and errors for various conditions such as empty portfolio data,
        failed goal mappings, and duplicate fund mappings.
    """
    st.set_page_config(page_title="Goal Mapping Tool", layout="wide")
    st.title("Investment Goal Mapping Tool")

    # Check and update database schema
    check_and_update_schema()

    # Get current portfolio data
    portfolio_df = get_portfolio_data()
    
    # Create tabs for different types of investments
    tab1, tab2 = st.tabs(["Mutual Fund Mapping", "Manual Investment Entry"])
    
    with tab1:
        if portfolio_df.empty:
            st.warning("No portfolio data found. Please ensure your portfolio data is up to date.")
        else:
            st.subheader("Map Mutual Fund to Goal")
            with st.form("goal_mapping_form"):
                goal_name = st.text_input("Goal Name")
                investment_type = st.selectbox("Investment Type", ["Equity", "Debt"])
                
                # Create scheme selection dropdown with current values
                scheme_options = portfolio_df.apply(
                    lambda x: f"{x['scheme_name']} ({format_indian_currency(x['current_value'])})", 
                    axis=1
                ).tolist()
                selected_scheme = st.selectbox("Select Fund", scheme_options)
                
                # Extract scheme details from selection
                if selected_scheme:
                    scheme_name = selected_scheme.split(" (₹")[0]
                    scheme_details = portfolio_df[portfolio_df['scheme_name'] == scheme_name].iloc[0]
                    current_value = scheme_details['current_value']
                    scheme_code = scheme_details['scheme_code']
                
                submitted = st.form_submit_button("Map to Goal")
                
                if submitted and goal_name and investment_type and selected_scheme:
                    # Check if fund is already mapped
                    existing_goal = check_existing_mapping(scheme_name, scheme_code)
                    if existing_goal:
                        st.error(f"This fund is already mapped to goal: {existing_goal}")
                    else:
                        insert_success = insert_goal_mapping(
                            goal_name, 
                            investment_type, 
                            scheme_name,
                            scheme_code,
                            current_value
                        )
                        if insert_success:
                            st.success(f"Successfully mapped {scheme_name} to goal: {goal_name}")
                        else:
                            st.error("Failed to map investment to goal. Please try again.")

    with tab2:
        st.subheader("Add Manual Investment")
        with st.form("manual_investment_form"):
            manual_goal_name = st.text_input("Goal Name")
            manual_scheme_type = st.selectbox(
                "Investment Type",
                ["PPF", "EPF", "NPS", "Fixed Deposit", "Other"]
            )
            manual_scheme_description = st.text_input(
                "Description",
                help="Enter additional details like FD duration, bank name etc."
            )
            manual_amount = st.number_input("Current Value", min_value=0.0, step=1000.0)
            
            manual_submitted = st.form_submit_button("Add Investment")
            
            if manual_submitted and manual_goal_name and manual_scheme_description and manual_amount > 0:
                # For manual entries: scheme_name is the investment type, scheme_code is 9999
                insert_success = insert_goal_mapping(
                    manual_goal_name,
                    "Debt",  # Fixed as Debt for all manual entries
                    manual_scheme_type,  # Use the investment type as scheme_name
                    "9999",  # Fixed scheme_code for manual debt instruments
                    manual_amount,
                    is_manual_entry=True
                )
                if insert_success:
                    st.success(f"Successfully added {manual_scheme_type} investment to goal: {manual_goal_name}")
                else:
                    st.error("Failed to add investment. Please try again.")

    # Display existing goal mappings
    st.subheader("Current Goal Mappings")
    existing_goals = get_existing_goals()
    
    if not existing_goals.empty:
        # Group by goal and calculate total value
        goal_summary = existing_goals.groupby('goal_name').agg({
            'current_value': 'sum'
        }).reset_index()
        
        # Display goal summary
        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader("Goal-wise Summary")
            summary_cols = st.columns(len(goal_summary))
            for idx, goal in goal_summary.iterrows():
                with summary_cols[idx]:
                    st.metric(
                        goal['goal_name'],
                        format_indian_currency(goal['current_value'])
                    )
        
        with col2:
            st.subheader("Total Portfolio Value")
            st.metric(
                "Total",
                format_indian_currency(goal_summary['current_value'].sum())
            )
        
        # Display detailed mappings
        st.subheader("Detailed Mappings")
        # Format the dataframe for display
        display_df = existing_goals.copy()
        display_df['current_value'] = display_df['current_value'].apply(format_indian_currency)
        display_df['Source'] = display_df['is_manual_entry'].map({True: 'Manual Entry', False: 'Portfolio'})
        display_df = display_df.drop('is_manual_entry', axis=1)
        st.dataframe(display_df)
    else:
        st.info("No goal mappings exist yet. Use the forms above to create your first mapping.")

if __name__ == "__main__":
    main()