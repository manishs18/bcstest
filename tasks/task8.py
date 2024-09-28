import psycopg2
import pandas as pd
from datetime import timedelta

# Database configuration
database = "bcs"
user = "postgres"
password = "password"
host = "localhost"
port = "5432"

def get_connection():
    try:
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Connection Established...")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_call_options_data(conn):
    """Fetch Call options data from the database."""
    query = """
    SELECT "Timestamp", "Bid Price", "Ask Price", "Volume", "Open Interest"
    FROM option_chain_tick_data
    WHERE "Option Type" = 'Call'
    ORDER BY "Timestamp";
    """
    return pd.read_sql_query(query, conn)

def backtest_call_options(df):
    """Backtest the buying and selling of Call options based on specified rules."""
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    trades = []

    for i in range(len(df) - 1):
        current_time = df.iloc[i]['Timestamp']
        current_bid_price = df.iloc[i]['Bid Price']
        current_ask_price = df.iloc[i]['Ask Price']

        # Calculate Last Traded Price (LTP) as average of Bid and Ask
        current_ltp = (current_bid_price + current_ask_price) / 2

        # Look for LTP increase of 5% within the next 10 minutes
        future_prices = df[(df['Timestamp'] > current_time) & (df['Timestamp'] <= current_time + timedelta(minutes=10))]

        if not future_prices.empty:
            future_bid_price = future_prices.iloc[0]['Bid Price']
            future_ask_price = future_prices.iloc[0]['Ask Price']
            future_ltp = (future_bid_price + future_ask_price) / 2

            print(f"Current LTP: {current_ltp}, Future LTP: {future_ltp}")

            if future_ltp >= current_ltp * 1.05:  # 5% increase condition
                entry_time = current_time
                entry_price = current_bid_price
                print(f"Buying Call at {entry_price} on {entry_time}")

                # Now look for the exit conditions
                exit_price = entry_price * 0.97  # Selling when LTP decreases by 3%
                exit_time = None

                for j in range(i, len(df)):
                    future_ltp = (df.iloc[j]['Bid Price'] + df.iloc[j]['Ask Price']) / 2
                    if future_ltp <= exit_price:
                        exit_time = df.iloc[j]['Timestamp']
                        print(f"Selling Call at {future_ltp} on {exit_time}")
                        trades.append((entry_time, exit_time, entry_price, future_ltp))
                        break
                
                # If no exit price hit, exit at the end of the day
                if exit_time is None:
                    exit_time = df.iloc[-1]['Timestamp']
                    exit_price = (df.iloc[-1]['Bid Price'] + df.iloc[-1]['Ask Price']) / 2
                    trades.append((entry_time, exit_time, entry_price, exit_price))
                    print(f"End of day exit at {exit_price} on {exit_time}")

    return trades

if __name__ == "__main__":
    connection = get_connection()
    
    if connection:
        # Fetch call options data
        call_data = fetch_call_options_data(connection)
        
        if not call_data.empty:
            print(f"Total Call Options fetched: {len(call_data)}")
            # Run backtesting
            trades = backtest_call_options(call_data)
            print("\nSummary of Trades:")
            for trade in trades:
                print(f"Entry: {trade[0]}, Exit: {trade[1]}, Entry Price: {trade[2]}, Exit Price: {trade[3]}")
        else:
            print("No Call options data found.")

        # Close the connection
        connection.close()
