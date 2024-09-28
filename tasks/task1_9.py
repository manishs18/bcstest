import bcsdbconfig  # Ensure this is correctly configured with your database details
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{bcsdbconfig.user}:{bcsdbconfig.password}@{bcsdbconfig.host}:{bcsdbconfig.port}/{bcsdbconfig.database}')

if __name__ == "__main__":
    # Task 1: Fetch data from PostgreSQL tables
    nifty_data = pd.read_sql_query('SELECT * FROM nifty_tick_data', engine)
    option_chain_data = pd.read_sql_query('SELECT * FROM option_chain_tick_data', engine)

    # Descriptive statistics for Nifty Tick Data
    nifty_stats = nifty_data[['Open', 'Close', 'Volume']].describe()
    print("Nifty Tick Data Statistics:\n", nifty_stats)

    # Descriptive statistics for Option Chain Data
    option_stats = option_chain_data[['Strike Price', 'Volume', 'Open Interest']].describe()
    print("\nOption Chain Data Statistics:\n", option_stats)

    # Task 2: Total volume for each option type
    total_volume = option_chain_data.groupby('Option Type')['Volume'].sum().reset_index()
    total_volume.columns = ['Option Type', 'Total Volume']
    print("\nTotal Volume for each Option Type:\n", total_volume)

    # Task 3: Remove rows with missing or inconsistent data
    option_chain_data_cleaned = option_chain_data.dropna()

    # Task 4: Format date columns
    option_chain_data_cleaned['Timestamp'] = pd.to_datetime(option_chain_data_cleaned['Timestamp'])
    option_chain_data_cleaned = option_chain_data_cleaned.sort_values(by='Timestamp')

    # Debug: Check the columns and data in the cleaned DataFrame
    print("\nColumns in option_chain_data_cleaned:\n", option_chain_data_cleaned.columns)
    print("\nSample Data:\n", option_chain_data_cleaned.head())

    # Task 5: Filter options with Strike Price > 15000 and Volume > 1000
    filtered_options = option_chain_data_cleaned[(option_chain_data_cleaned['Strike Price'] > 15000) &
                                                 (option_chain_data_cleaned['Volume'] > 1000)]
    print("\nFiltered Options:\n", filtered_options)

    # Check if the 'Option' column exists before filtering by it
    if 'Option' in option_chain_data_cleaned.columns:
        specific_option = 'NIFTY 15000 CE'  # Change as needed
        specific_option_data = option_chain_data_cleaned[option_chain_data_cleaned['Option'] == specific_option]

        plt.figure(figsize=(10, 5))
        plt.plot(specific_option_data['Timestamp'], specific_option_data['Bid Price'], marker='o')
        plt.title(f'Last Traded Price of {specific_option}')
        plt.xlabel('Timestamp')
        plt.ylabel('Bid Price')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    else:
        print("The 'Option' column does not exist in the option_chain_data_cleaned DataFrame.")

    # Task 8: Backtesting rule for buying a Call option
    potential_trades = []
    for i in range(1, len(option_chain_data_cleaned)):
        current_row = option_chain_data_cleaned.iloc[i]
        previous_row = option_chain_data_cleaned.iloc[i - 1]

        # Ensure 'Bid Price' exists in current and previous rows
        if 'Bid Price' in current_row and 'Bid Price' in previous_row:
            if (current_row['Option Type'] == 'Call' and
                current_row['Bid Price'] >= previous_row['Bid Price'] * 1.05 and
                (current_row['Timestamp'] - previous_row['Timestamp']).seconds <= 600):
                potential_trades.append({
                    'Timestamp': current_row['Timestamp'],
                    'Bid Price': current_row['Bid Price'],
                    'Previous Bid Price': previous_row['Bid Price']
                })

    # Check if any trades were found before proceeding
    if potential_trades:
        # Task 9: Calculate and plot the performance of the strategy
        trade_performance = pd.DataFrame(potential_trades)
        trade_performance['Profit/Loss'] = trade_performance['Bid Price'] - trade_performance['Previous Bid Price']
        trade_performance['Cumulative Profit'] = trade_performance['Profit/Loss'].cumsum()

        plt.figure(figsize=(10, 5))
        plt.plot(trade_performance['Timestamp'], trade_performance['Cumulative Profit'], marker='o')
        plt.title('Cumulative Profit/Loss Over Time')
        plt.xlabel('Timestamp')
        plt.ylabel('Cumulative Profit')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    else:
        print("No potential trades found based on the criteria.")

    # Close the database connection
    engine.dispose()
