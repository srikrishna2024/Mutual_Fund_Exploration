import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

@st.cache_resource
def init_connection():
    try:
        return psycopg.connect(**DB_PARAMS)
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

def create_mapping_table():
    """Create fund_mapping table if it doesn't exist"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS fund_mapping (
                        id SERIAL PRIMARY KEY,
                        fund_code VARCHAR(50) NOT NULL,
                        scheme_name VARCHAR(255) NOT NULL,
                        goal_id INTEGER,
                        retirement_id INTEGER,
                        allocation_percentage NUMERIC(5, 2) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (goal_id) REFERENCES goal_planner(id),
                        FOREIGN KEY (retirement_id) REFERENCES retirement(id)
                    );
                """)
                conn.commit()
        except Exception as e:
            st.error(f"Failed to create mapping table: {str(e)}")

def get_unique_funds():
    """Get list of unique funds from portfolio_data"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT DISTINCT code, scheme_name
                FROM portfolio_data
                ORDER BY scheme_name;
            """
            return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Failed to retrieve funds: {str(e)}")
            return pd.DataFrame()

def get_goals():
    """Get list of goals from goal_planner"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT id, goal_name, future_cost
                FROM goal_planner
                ORDER BY created_at DESC;
            """
            return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Failed to retrieve goals: {str(e)}")
            return pd.DataFrame()

def get_retirement_plans():
    """Get list of retirement plans"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT id, analysis_date, required_retirement_corpus
                FROM retirement
                ORDER BY created_at DESC;
            """
            return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Failed to retrieve retirement plans: {str(e)}")
            return pd.DataFrame()

def get_existing_mappings():
    """Get existing fund mappings with goal and retirement information"""
    conn = init_connection()
    if conn:
        try:
            query = """
                SELECT 
                    fm.fund_code,
                    fm.scheme_name,
                    fm.goal_id,
                    fm.retirement_id,
                    fm.allocation_percentage,
                    gp.goal_name,
                    r.analysis_date
                FROM fund_mapping fm
                LEFT JOIN goal_planner gp ON fm.goal_id = gp.id
                LEFT JOIN retirement r ON fm.retirement_id = r.id
                ORDER BY fm.created_at DESC;
            """
            df = pd.read_sql_query(query, conn)
            # Replace NaN values with None for better display
            return df.where(pd.notna(df), None)
        except Exception as e:
            st.error(f"Failed to retrieve mappings: {str(e)}")
            return pd.DataFrame()

def save_mapping(fund_code, scheme_name, goal_id, retirement_id, allocation):
    """Save fund mapping to database"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                # Delete existing mapping for this fund
                cur.execute("""
                    DELETE FROM fund_mapping
                    WHERE fund_code = %s;
                """, (fund_code,))
                
                # Insert new mapping
                cur.execute("""
                    INSERT INTO fund_mapping 
                    (fund_code, scheme_name, goal_id, retirement_id, allocation_percentage)
                    VALUES (%s, %s, %s, %s, %s);
                """, (fund_code, scheme_name, goal_id, retirement_id, allocation))
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Failed to save mapping: {str(e)}")
            return False

def main():
    st.set_page_config(
        page_title="Portfolio Fund Mapper",
        page_icon="🎯",
        layout="wide"
    )
    
    st.title("Portfolio Fund Mapper")
    
    # Create mapping table if it doesn't exist
    create_mapping_table()
    
    # Get data
    funds_df = get_unique_funds()
    goals_df = get_goals()
    retirement_df = get_retirement_plans()
    existing_mappings = get_existing_mappings()
    
    if funds_df.empty:
        st.warning("No funds found in the portfolio. Please add some transactions first.")
        return
    
    # Create mapping interface
    st.subheader("Map Funds to Goals and Retirement")
    
    for _, fund in funds_df.iterrows():
        with st.expander(f"{fund['scheme_name']} ({fund['code']})"):
            existing_mapping = existing_mappings[
                existing_mappings['fund_code'] == fund['code']
            ].iloc[0] if not existing_mappings[
                existing_mappings['fund_code'] == fund['code']
            ].empty else None
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Goal selection
                goal_options = {
                    f"{row['goal_name']} (₹{row['future_cost']:,.2f})": row['id'] 
                    for _, row in goals_df.iterrows()
                } if not goals_df.empty else {}
                goal_options['None'] = None
                
                selected_goal = st.selectbox(
                    "Map to Goal",
                    options=list(goal_options.keys()),
                    key=f"goal_{fund['code']}",
                    index=list(goal_options.keys()).index('None') if existing_mapping is None 
                    else list(goal_options.values()).index(existing_mapping['goal_id'])
                    if existing_mapping is not None and existing_mapping['goal_id'] is not None 
                    else list(goal_options.keys()).index('None')
                )
            
            with col2:
                # Retirement plan selection
                retirement_options = {
                    f"Plan {row['analysis_date']} (₹{row['required_retirement_corpus']:,.2f})": row['id']
                    for _, row in retirement_df.iterrows()
                } if not retirement_df.empty else {}
                retirement_options['None'] = None
                
                selected_retirement = st.selectbox(
                    "Map to Retirement Plan",
                    options=list(retirement_options.keys()),
                    key=f"retirement_{fund['code']}",
                    index=list(retirement_options.keys()).index('None') if existing_mapping is None 
                    else list(retirement_options.values()).index(existing_mapping['retirement_id'])
                    if existing_mapping is not None and existing_mapping['retirement_id'] is not None 
                    else list(retirement_options.keys()).index('None')
                )
            
            with col3:
                # Allocation percentage
                allocation = st.number_input(
                    "Allocation Percentage",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(existing_mapping['allocation_percentage']) if existing_mapping is not None else 100.0,
                    key=f"allocation_{fund['code']}"
                )
            
            if st.button("Save Mapping", key=f"save_{fund['code']}"):
                goal_id = goal_options[selected_goal]
                retirement_id = retirement_options[selected_retirement]
                
                if goal_id is None and retirement_id is None:
                    st.error("Please select either a goal or retirement plan")
                else:
                    if save_mapping(fund['code'], fund['scheme_name'], goal_id, retirement_id, allocation):
                        st.success("Mapping saved successfully!")
                        st.rerun()
    
    # Display current mappings
    st.subheader("Current Fund Mappings")
    if not existing_mappings.empty:
        # Create display dataframe with proper column names
        display_df = pd.DataFrame({
            'Scheme Name': existing_mappings['scheme_name'],
            'Fund Code': existing_mappings['fund_code'],
            'Mapped Goal': existing_mappings['goal_name'].fillna('Not mapped'),
            'Mapped Retirement Plan': existing_mappings['analysis_date'].fillna('Not mapped'),
            'Allocation %': existing_mappings['allocation_percentage']
        })
        
        st.dataframe(display_df)
    else:
        st.info("No mappings created yet. Use the form above to map your funds.")

if __name__ == "__main__":
    main()
