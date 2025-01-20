import streamlit as st
import pandas as pd
import psycopg
from datetime import datetime

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

def update_schema_for_sync():
    """Add last_synced_at column if it doesn't exist"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'goals' AND column_name = 'last_synced_at'
                );
            """)
            has_last_synced = cur.fetchone()[0]
            
            if not has_last_synced:
                cur.execute("""
                    ALTER TABLE goals 
                    ADD COLUMN last_synced_at TIMESTAMP DEFAULT NULL
                """)
            
        conn.commit()

def get_latest_transactions(last_sync_date=None):
    """Get all transactions since the last sync date"""
    with connect_to_db() as conn:
        query = """
            WITH latest_nav AS (
                SELECT code, value, nav
                FROM mutual_fund_nav
                WHERE (code, nav) IN (
                    SELECT code, MAX(nav)
                    FROM mutual_fund_nav
                    GROUP BY code
                )
            )
            SELECT 
                p.scheme_name,
                p.code as scheme_code,
                p.transaction_type,
                p.units,
                p.created_at as transaction_date,
                n.value as current_nav
            FROM portfolio_data p
            JOIN latest_nav n ON p.code = n.code
            WHERE 1=1
        """
        
        if last_sync_date:
            query += " AND p.created_at > %s"
            df = pd.read_sql(query, conn, params=[last_sync_date])
        else:
            df = pd.read_sql(query, conn)
            
        return df

def update_goal_values():
    """Update current values in goals table based on latest portfolio data"""
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            portfolio_query = """
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
            
            portfolio_df = pd.read_sql(portfolio_query, conn)
            
            # Update each mapped fund with its latest value
            for _, row in portfolio_df.iterrows():
                cur.execute("""
                    UPDATE goals 
                    SET current_value = %s,
                        last_synced_at = CURRENT_TIMESTAMP
                    WHERE scheme_code = %s 
                    AND is_manual_entry = FALSE
                """, (row['current_value'], row['scheme_code']))
            
            conn.commit()
            return len(portfolio_df)

def get_sync_summary():
    """Get summary of last sync status for all goals"""
    with connect_to_db() as conn:
        query = """
            SELECT 
                goal_name,
                scheme_name,
                scheme_code,
                current_value,
                created_at,
                last_synced_at,
                is_manual_entry
            FROM goals
            ORDER BY goal_name, scheme_name
        """
        return pd.read_sql(query, conn)

def get_total_by_goal(sync_summary):
    """Calculate total value for each goal"""
    goal_totals = sync_summary.groupby('goal_name')['current_value'].sum().reset_index()
    goal_totals['current_value'] = goal_totals['current_value'].apply(format_indian_currency)
    return goal_totals

def main():
    """
    Main function to run the Portfolio Sync Tool application.
    This function sets up the Streamlit page configuration, displays the current sync status,
    and provides functionality to sync the portfolio with the latest transactions.
    The main steps include:
    - Setting the page title and layout.
    - Displaying the current sync status in a formatted table.
    - Showing goal-wise totals.
    - Providing a sync button to update the portfolio with the latest transactions.
    - Displaying appropriate messages based on the sync status and results.
    Functions called:
    - update_schema_for_sync(): Ensures the database schema is up to date.
    - get_sync_summary(): Retrieves the current sync status summary.
    - format_indian_currency(value): Formats currency values in Indian format.
    - get_total_by_goal(sync_summary): Calculates total values by goal.
    - get_latest_transactions(last_sync_date): Retrieves new transactions since the last sync date.
    - update_goal_values(): Updates the goal values with the latest NAVs.
    Streamlit components used:
    - st.set_page_config(): Sets the page configuration.
    - st.title(): Displays the main title.
    - st.subheader(): Displays subheaders.
    - st.dataframe(): Displays dataframes.
    - st.info(): Displays informational messages.
    - st.button(): Creates a button.
    - st.success(): Displays success messages.
    - st.warning(): Displays warning messages.
    - st.rerun(): Reruns the Streamlit script.
    """
    st.set_page_config(page_title="Portfolio Sync Tool", layout="wide")
    st.title("Portfolio Sync Tool")

    # Ensure schema is up to date
    update_schema_for_sync()

    # Get current sync status
    sync_summary = get_sync_summary()
    
    # Display current status
    st.subheader("Current Sync Status")
    if not sync_summary.empty:
        # Format the display dataframe
        display_df = sync_summary.copy()
        display_df['current_value'] = display_df['current_value'].apply(format_indian_currency)
        display_df['last_sync'] = display_df['last_synced_at'].apply(
            lambda x: 'Never' if pd.isna(x) else x.strftime('%Y-%m-%d %H:%M')
        )
        display_df = display_df.drop(['created_at', 'last_synced_at'], axis=1)
        
        # Reorder and rename columns for better display
        display_df = display_df[[
            'goal_name', 
            'scheme_name', 
            'scheme_code', 
            'current_value', 
            'last_sync', 
            'is_manual_entry'
        ]].rename(columns={
            'goal_name': 'Goal',
            'scheme_name': 'Scheme',
            'scheme_code': 'Code',
            'current_value': 'Current Value (Lakhs)',
            'last_sync': 'Last Synced',
            'is_manual_entry': 'Manual Entry'
        })
        
        st.dataframe(display_df, hide_index=True)

        # Display goal-wise totals
        st.subheader("Goal-wise Totals")
        goal_totals = get_total_by_goal(sync_summary)
        st.dataframe(
            goal_totals.rename(columns={
                'goal_name': 'Goal',
                'current_value': 'Total Value (Lakhs)'
            }),
            hide_index=True
        )

        # Get the earliest last sync date
        last_sync_date = sync_summary['last_synced_at'].min()
        
        # Sync button with status
        col1, col2 = st.columns([2, 1])
        with col1:
            if pd.isna(last_sync_date):
                st.info("No previous sync found. Will sync all transactions.")
            else:
                st.info(f"Last sync was on: {last_sync_date.strftime('%Y-%m-%d %H:%M')}")

        with col2:
            if st.button("Sync Portfolio"):
                # Get new transactions for display
                new_transactions = get_latest_transactions(last_sync_date)
                num_new_transactions = len(new_transactions)
                
                # Update all goals with latest values
                num_funds_updated = update_goal_values()
                
                # Show success message with summary
                if num_new_transactions > 0:
                    st.success(f"Successfully synced {num_new_transactions} new transactions across {num_funds_updated} funds!")
                else:
                    st.success(f"Updated NAV values for {num_funds_updated} funds. No new transactions found.")
                
                # Refresh the sync status table
                st.rerun()
    else:
        st.warning("No goals mapped yet. Please map your investments to goals first.")

if __name__ == "__main__":
    main()