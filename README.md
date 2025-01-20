# Mutual Fund Exploration

This repository contains Python scripts organized into various subfolders. This document provides a summary of the scripts, including descriptions of their functions, classes, and usage instructions based on the generated `documentation.md` file.

## Table of Contents

- [`create_documentation.py`](#createdocumentationpy)
- [`requirements.py`](#requirementspy)
- [`1_update_master_Data.py`](#1updatemasterdatapy)
- [`2_nav_updater.py`](#2navupdaterpy)
- [`3_mutual_fund_delta_update.py`](#3mutualfunddeltaupdatepy)
- [`10_portfolio-risk-analysis.py`](#10portfolio-risk-analysispy)
- [`11_volatility-analysis-app.py`](#11volatility-analysis-apppy)
- [`13_portfolio-value-tracker.py`](#13portfolio-value-trackerpy)
- [`5_portfolio-upload.py`](#5portfolio-uploadpy)
- [`6_portfolio-analysis.py`](#6portfolio-analysispy)
- [`7_goal-mapping-tool.py`](#7goal-mapping-toolpy)
- [`8_portfolio-sync-tool.py`](#8portfolio-sync-toolpy)
- [`9_goal-planner.py`](#9goal-plannerpy)
- [`12_mutual_fund_consistency.py`](#12mutualfundconsistencypy)
- [`14_mutual-fund-category_analysis.py`](#14mutual-fund-categoryanalysispy)
- [`4_single_fund_analysis.py`](#4singlefundanalysispy)

# Documentation of Python Scripts in Folder

## `create_documentation.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\create_documentation.py
```
- **Functions**:
  - `def get_functions_and_classes()`
    - Description: Extract all functions and classes from a Python file, along with docstrings.
  - `def scan_folder_for_python_files()`
    - Description: Recursively scan folder and subfolders for Python scripts.
  - `def generate_documentation()`
    - Description: Generate documentation of all Python scripts in the folder.
  - `def main()`
    - Description: No description available.
- **Classes**:
  - None

## `requirements.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\requirements.py
```
- **Functions**:
  - `def get_imports_from_file()`
    - Description: Extract all imports from a Python file.
  - `def scan_folder_for_python_files()`
    - Description: Recursively scan folder and subfolders for Python scripts.
  - `def generate_requirements_txt()`
    - Description: Generate a requirements.txt file for all Python dependencies.
  - `def install_requirements()`
    - Description: Install all packages listed in requirements.txt.
  - `def main()`
    - Description: No description available.
- **Classes**:
  - None

## `1_update_master_Data.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\1. MF Data Update\1_update_master_Data.py
```
- **Functions**:
  - `def upload_csv_to_postgresql()`
    - Description: Uploads data from a CSV file to a PostgreSQL database table.
This function connects to a PostgreSQL database using the provided configuration,
checks if the 'mutual_fund_master_data' table exists, creates it if it doesn't,
and then uploads data from the specified CSV file into the table. The function
ensures that date columns are properly parsed and handles missing or invalid
dates by setting them to '9999-12-31'.
Args:
    file_path (str): The path to the CSV file containing the data to be uploaded.
    db_config (dict): A dictionary containing the database configuration with keys:
        - 'dbname': The name of the database.
        - 'user': The username used to authenticate.
        - 'password': The password used to authenticate.
        - 'host': The host address of the database.
        - 'port': The port number on which the database is listening.
Raises:
    Exception: If any error occurs during the process, it will be caught and printed.
- **Classes**:
  - None

## `2_nav_updater.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\1. MF Data Update\2_nav_updater.py
```
- **Functions**:
  - `def parse_date()`
    - Description: Parses a date string in DD-MM-YYYY format.
  - `def create_nav_table_if_not_exists()`
    - Description: Creates the mutual_fund_nav table if it doesn't exist.
  - `def fetch_open_ended_schemes()`
    - Description: Fetches all open-ended schemes.
  - `def fetch_nav_data()`
    - Description: Fetches NAV data for a specific scheme using MFAPI with retry logic.
  - `def update_nav_data()`
    - Description: Updates NAV data for the given list of schemes.
  - `def read_last_downloaded_scheme()`
    - Description: Reads the last downloaded scheme code from the log file.
  - `def write_last_downloaded_scheme()`
    - Description: Writes the last downloaded scheme code to the log file.
  - `def nav_updater()`
    - Description: Updates the Net Asset Value (NAV) data for mutual fund schemes in a PostgreSQL database.
Args:
    db_config (dict): A dictionary containing the database configuration with keys:
        - 'dbname': The name of the database.
        - 'user': The username used to authenticate.
        - 'password': The password used to authenticate.
        - 'host': The host address of the database.
        - 'port': The port number on which the database is listening.
The function performs the following steps:
    1. Connects to the PostgreSQL database using the provided configuration.
    2. Ensures that the NAV table exists in the database.
    3. Fetches all eligible open-ended mutual fund schemes.
    4. Prompts the user to choose an update option:
        - Update all schemes.
        - Update 5000 schemes starting from the last downloaded scheme.
        - Update a specific scheme based on the scheme code.
    5. Updates the NAV data based on the user's choice.
    6. Commits the transaction to the database.
Raises:
    Exception: If any error occurs during the database connection or data update process, it prints the error message.
- **Classes**:
  - None

## `3_mutual_fund_delta_update.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\1. MF Data Update\3_mutual_fund_delta_update.py
```
- **Functions**:
  - `def write_log()`
    - Description: Writes a message to the log file.
  - `def fetch_schemes_to_update()`
    - Description: Fetches schemes from the mutual_fund_nav table where the most recent NAV date 
is within the last 30 days. If a specific scheme code is provided, 
only fetch that scheme.
  - `def fetch_nav_data()`
    - Description: Fetches NAV data for a specific scheme using MFAPI with retry logic.
  - `def update_nav_data()`
    - Description: Updates NAV data for the given list of schemes.
  - `def nav_recent_updater()`
    - Description: Updates the Net Asset Value (NAV) data for mutual fund schemes in a PostgreSQL database.
Parameters:
db_config (dict): A dictionary containing the database configuration with keys:
    - 'dbname': Name of the database
    - 'user': Username for the database
    - 'password': Password for the database
    - 'host': Host address of the database
    - 'port': Port number of the database
The function provides two options to the user:
1. Update all schemes
2. Update a specific scheme by entering its scheme code
The function fetches the schemes to update based on the user's choice and updates their NAV data.
It commits the changes to the database and logs the process.
If an error occurs during the process, it prints the error message and logs it.
Note:
- The function assumes the existence of helper functions `fetch_schemes_to_update`, `update_nav_data`, and `write_log`.
- The function uses the `psycopg` library to connect to the PostgreSQL database.
- **Classes**:
  - None

## `10_portfolio-risk-analysis.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\10_portfolio-risk-analysis.py
```
- **Functions**:
  - `def format_indian_number()`
    - Description: Format a number in Indian style (lakhs, crores)
  - `def connect_to_db()`
    - Description: Create database connection
  - `def get_portfolio_data()`
    - Description: Retrieve all records from portfolio_data table
  - `def get_portfolio_funds()`
    - Description: Get list of funds currently in portfolio
  - `def get_latest_nav()`
    - Description: Retrieve the latest NAVs for portfolio funds
  - `def get_historical_nav()`
    - Description: Retrieve historical NAV data for portfolio funds
  - `def prepare_cashflows()`
    - Description: Prepare cashflow data from portfolio transactions
  - `def xirr()`
    - Description: Calculate XIRR given a set of transactions
  - `def calculate_portfolio_weights()`
    - Description: Calculate current portfolio weights for each scheme
  - `def calculate_xirr()`
    - Description: Calculate XIRR for portfolio and individual schemes
  - `def calculate_returns()`
    - Description: Calculate historical returns for portfolio funds
  - `def calculate_portfolio_metrics()`
    - Description: Calculate portfolio risk metrics
  - `def interpret_portfolio_metrics()`
    - Description: Generate insights from portfolio metrics
  - `def main()`
    - Description: Main function to run the Portfolio Risk Analysis Dashboard.
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
  - `def xnpv()`
    - Description: No description available.
  - `def xnpv_der()`
    - Description: No description available.
- **Classes**:
  - None

## `11_volatility-analysis-app.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\11_volatility-analysis-app.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def get_portfolio_data()`
    - Description: Retrieve portfolio data
  - `def get_historical_nav()`
    - Description: Retrieve historical NAV data
  - `def calculate_fund_metrics()`
    - Description: Calculate volatility metrics for each fund
  - `def analyze_risk_factors()`
    - Description: Analyze why a fund has high risk contribution
  - `def main()`
    - Description: Main function to run the Portfolio Volatility Analysis application.
This function sets up the Streamlit page configuration, loads portfolio data,
calculates volatility metrics, and displays various analyses and visualizations
related to portfolio risk and fund performance.
The function performs the following steps:
1. Sets the page title and layout.
2. Loads portfolio data and checks for its availability.
3. Loads historical NAV data for the portfolio funds.
4. Calculates volatility metrics, correlation matrix, and portfolio volatility.
5. Displays portfolio risk overview including annualized volatility and highest risk contribution.
6. Displays individual fund analysis with metrics and primary risk factors.
7. Visualizes risk contribution analysis using bar charts.
8. Displays fund correlation analysis using a heatmap.
9. Handles exceptions and displays error messages if any issues occur.
Raises:
    Exception: If there is an error in loading data, calculating metrics, or any other step.
- **Classes**:
  - None

## `13_portfolio-value-tracker.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\13_portfolio-value-tracker.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection with error handling
  - `def get_available_funds()`
    - Description: Get list of unique funds from portfolio_data table
  - `def get_fund_nav_data()`
    - Description: Get historical NAV data for selected funds
  - `def get_fund_transactions()`
    - Description: Get transaction data for selected funds
  - `def xirr()`
    - Description: Calculate XIRR given a set of cash flows
  - `def calculate_fund_xirr()`
    - Description: Calculate XIRR for a specific fund or total portfolio
If fund_code is None, calculates for entire portfolio
  - `def calculate_portfolio_value()`
    - Description: Calculate daily portfolio value for each fund
  - `def format_value()`
    - Description: Format currency values in Indian format (with lakhs and crores)
  - `def calculate_time_based_returns()`
    - Description: Calculate absolute value change over specified number of months
  - `def main()`
    - Description: Main function to run the Portfolio Value Tracker Streamlit application.
This function sets up the Streamlit page configuration, retrieves available funds,
allows the user to select funds to track, fetches NAV and transaction data for the
selected funds, calculates portfolio values, and displays various metrics and plots.
The function performs the following steps:
1. Sets the page title and layout.
2. Retrieves available funds from the database.
3. Allows the user to select funds to track using a multi-select widget.
4. Fetches NAV and transaction data for the selected funds.
5. Calculates portfolio values over time.
6. Plots the portfolio value progression over time.
7. Calculates and displays time-based returns (1-month, 3-month, 6-month).
8. Displays metrics for each selected fund, including current value and XIRR.
9. Displays total portfolio metrics, including total value, portfolio XIRR, and gains/losses.
10. Provides an expandable section to view raw data.
If any errors occur during the execution, they are caught and displayed as error messages.
Raises:
    Exception: If any error occurs during the execution of the function.
  - `def xnpv()`
    - Description: Calculate XNPV given a rate and cashflows
  - `def xirr_objective()`
    - Description: No description available.
- **Classes**:
  - None

## `5_portfolio-upload.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\5_portfolio-upload.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def create_portfolio_table()`
    - Description: Create portfolio_data table if it doesn't exist
  - `def clean_numeric_data()`
    - Description: Clean numeric columns by removing commas and converting to numeric
  - `def validate_dataframe()`
    - Description: Validate the uploaded dataframe format and data
  - `def insert_portfolio_data()`
    - Description: Insert validated data into portfolio_data table
  - `def get_portfolio_data()`
    - Description: Retrieve all records from portfolio_data table
  - `def main()`
    - Description: Main function to handle the Portfolio Transaction Upload page.
This function sets up the Streamlit page configuration, handles file uploads,
validates and processes the uploaded data, and displays the current portfolio data.
Steps:
1. Set up the Streamlit page configuration and title.
2. Create the portfolio table if it doesn't exist.
3. Provide a file uploader for CSV or Excel files.
4. Read and process the uploaded file.
5. Validate the data and show a preview.
6. Insert the validated data into the database upon user confirmation.
7. Display the current portfolio data from the database.
Raises:
    Exception: If there is an error processing the uploaded file or inserting data into the database.
- **Classes**:
  - None

## `6_portfolio-analysis.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\6_portfolio-analysis.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def get_portfolio_data()`
    - Description: Retrieve all records from portfolio_data table
  - `def get_latest_nav()`
    - Description: Retrieve the latest NAVs from mutual_fund_nav table
  - `def prepare_cashflows()`
    - Description: Prepare cashflow data from portfolio transactions
  - `def xirr()`
    - Description: Calculate XIRR given a set of transactions
  - `def calculate_portfolio_weights()`
    - Description: Calculate current portfolio weights for each scheme
  - `def calculate_xirr()`
    - Description: Calculate XIRR (Extended Internal Rate of Return) for a portfolio and individual schemes.

Parameters:
df (pd.DataFrame): DataFrame containing transaction details with columns 'scheme_name', 'date', 'units', 'cashflow', and 'code'.
latest_nav (pd.DataFrame): DataFrame containing the latest NAV values with columns 'code' and 'nav_value'.

Returns:
tuple: A tuple containing:
    - xirr_results (dict): A dictionary with scheme names as keys and their respective XIRR values as values. The key 'Portfolio' contains the overall portfolio XIRR.
    - portfolio_growth (pd.DataFrame): A DataFrame with columns 'date' and 'value' representing the portfolio value growth over time.
  - `def main()`
    - Description: No description available.
  - `def xnpv()`
    - Description: No description available.
  - `def xnpv_der()`
    - Description: No description available.
- **Classes**:
  - None

## `7_goal-mapping-tool.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\7_goal-mapping-tool.py
```
- **Functions**:
  - `def format_indian_currency()`
    - Description: Format amount in Indian currency style (lakhs, crores)
  - `def connect_to_db()`
    - Description: Create database connection
  - `def check_and_update_schema()`
    - Description: Check if goals table exists and has required columns, update if necessary
  - `def get_portfolio_data()`
    - Description: Retrieve current portfolio data with latest NAVs
  - `def check_existing_mapping()`
    - Description: Check if a fund is already mapped to any goal
  - `def insert_goal_mapping()`
    - Description: Insert a new goal mapping into the goals table
  - `def get_existing_goals()`
    - Description: Retrieve existing goal mappings
  - `def main()`
    - Description: Main function to run the Investment Goal Mapping Tool application.
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
  - `def format_number()`
    - Description: No description available.
- **Classes**:
  - None

## `8_portfolio-sync-tool.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\8_portfolio-sync-tool.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def format_indian_currency()`
    - Description: Format amount in lakhs with 2 decimal places
  - `def update_schema_for_sync()`
    - Description: Add last_synced_at column if it doesn't exist
  - `def get_latest_transactions()`
    - Description: Get all transactions since the last sync date
  - `def update_goal_values()`
    - Description: Update current values in goals table based on latest portfolio data
  - `def get_sync_summary()`
    - Description: Get summary of last sync status for all goals
  - `def get_total_by_goal()`
    - Description: Calculate total value for each goal
  - `def main()`
    - Description: Main function to run the Portfolio Sync Tool application.
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
- **Classes**:
  - None

## `9_goal-planner.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\2. Portfolio Analysis\9_goal-planner.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def format_indian_currency()`
    - Description: Format amount in lakhs with 2 decimal places
  - `def get_goals()`
    - Description: Get list of unique goals from goals table
  - `def get_current_investments()`
    - Description: Get current equity and debt investments for the goal
  - `def calculate_future_value()`
    - Description: Calculate future value considering inflation
  - `def calculate_retirement_corpus()`
    - Description: Calculate required retirement corpus with corrected calculation
  - `def calculate_required_investment()`
    - Description: Calculate yearly investment required with increasing contributions
  - `def create_investment_projection_plot()`
    - Description: Create projection plot comparing expected vs actual investments
  - `def main()`
    - Description: Main function to run the Investment Goal Planner application.
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
- **Classes**:
  - None

## `12_mutual_fund_consistency.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\3. MF Analysis\12_mutual_fund_consistency.py
```
- **Functions**:
  - None
- **Classes**:
  - None

## `14_mutual-fund-category_analysis.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\3. MF Analysis\14_mutual-fund-category_analysis.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def get_categories()`
    - Description: Fetch unique scheme categories
  - `def get_schemes_by_category()`
    - Description: Fetch schemes for selected category
  - `def get_nav_data()`
    - Description: Fetch NAV data for selected scheme
  - `def calculate_rolling_returns()`
    - Description: Calculate rolling returns for given window period
  - `def calculate_category_average()`
    - Description: Safely calculate category average for a given period
  - `def main()`
    - Description: Main function to run the Streamlit app for Category-wide Mutual Fund Analysis.
This function sets up the Streamlit page configuration, displays the title, and creates a sidebar for user inputs.
It allows users to select a mutual fund category and analyze the rolling returns and risk for funds within that category.
The analysis includes:
- Returns Summary
- Risk Summary
- Individual Fund Analysis
- Detailed Statistics
The function fetches and processes NAV data for the selected category and displays the results in various tabs.
Sidebar Inputs:
- Mutual Fund Category: Dropdown to select the mutual fund category.
- Analyze Category: Button to trigger the analysis.
Tabs:
- Returns Summary: Displays the returns summary for the selected category.
- Risk Summary: Displays the risk summary (standard deviation) for the selected category.
- Individual Fund Analysis: Allows detailed analysis of individual funds within the selected category.
- Detailed Statistics: Provides detailed statistics for all funds in the selected category, with an option to download the data as a CSV file.
- **Classes**:
  - None

## `4_single_fund_analysis.py`
### How to run this script:
Run the script using the following command:
```bash
python c:\Users\[USER]\Downloads\Mutual_Fund_PostGRES\updated scripts\3. MF Analysis\4_single_fund_analysis.py
```
- **Functions**:
  - `def connect_to_db()`
    - Description: Create database connection
  - `def get_categories()`
    - Description: Fetch unique scheme categories for open ended funds
  - `def get_schemes_by_category()`
    - Description: Fetch schemes for selected category
  - `def get_nav_data()`
    - Description: Fetch NAV data for selected scheme
  - `def calculate_returns()`
    - Description: Calculate returns for given window period
  - `def calculate_risk_metrics()`
    - Description: Calculate various risk metrics for the mutual fund:
- Annualized Return
- Annualized Volatility
- Sharpe Ratio (assuming risk-free rate of 4%)
- Maximum Drawdown
- Sortino Ratio
- Value at Risk (VaR)
  - `def calculate_rolling_returns()`
    - Description: Calculate rolling returns for given window period
  - `def single_fund_analysis()`
    - Description: No description available.
  - `def compare_funds()`
    - Description: Displays a Streamlit interface for comparing mutual funds based on selected criteria.
The function allows users to:
- Select a scheme category.
- Select up to 3 schemes within the chosen category for comparison.
- Choose an analysis period (e.g., YTD, 1 Year, 2 Years, etc.).
- Compare the selected schemes based on rolling returns and risk metrics.
The function fetches and processes the necessary data, and displays:
- A plot of rolling returns for the selected schemes.
- A table comparing risk metrics for the selected schemes.
Raises:
    Exception: If an error occurs during data fetching or processing.
Note:
    This function relies on several helper functions:
    - get_categories(): Fetches available scheme categories.
    - get_schemes_by_category(category): Fetches schemes for a given category.
    - get_nav_data(scheme_code, start_date): Fetches NAV data for a scheme.
    - calculate_rolling_returns(nav_data): Calculates rolling returns from NAV data.
    - calculate_risk_metrics(nav_data): Calculates risk metrics from NAV data.
  - `def main()`
    - Description: No description available.
- **Classes**:
  - None

