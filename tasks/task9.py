import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Database connection configuration
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
    query = """
    SELECT "Timestamp", "Bid Price", "Ask Price", "Volume"
    FROM option_chain_tick_data
    WHERE "Option Type" = 'Call'
    ORDER BY "Timestamp";
    """
    return pd.read_sql_query(query, conn)

def backtest_call_options(call_data):
    trades = []
    
    for i in range(len(call_data) - 1):
        current_row = call_data.iloc[i]
        current_ltp = current_row['Bid Price']
        current_time = current_row['Timestamp']
        
        print(f"Current LTP: {current_ltp} at {current_time}")

        # Check next rows within the 10-minute window
        for j in range(i + 1, len(call_data)):
            future_row = call_data.iloc[j]
            future_ltp = future_row['Bid Price']
            future_time = future_row['Timestamp']
            
            if future_time > current_time + timedelta(minutes=10):
                break
            
            # Check if LTP increased by 5%
            if future_ltp >= current_ltp * 1.05:
                print(f"Found 5% increase: {future_ltp} at {future_time}")
                # Assume we sell when LTP decreases by 3% or at the end of the day
                for k in range(j, len(call_data)):
                    sell_row = call_data.iloc[k]
                    sell_ltp = sell_row['Bid Price']
                    sell_time = sell_row['Timestamp']
                    
                    if sell_ltp <= future_ltp * 0.97 or sell_time.date() != current_time.date():
                        trades.append({
                            'Entry Time': current_time,
                            'Entry LTP': current_ltp,
                            'Exit Time': sell_time,
                            'Exit LTP': sell_ltp,
                            'Profit/Loss': sell_ltp - current_ltp
                        })
                        print(f"Trade executed: Entry {current_ltp} at {current_time}, Exit {sell_ltp} at {sell_time}")
                        break

    return trades

def calculate_performance(trades):
    performance = []
    cumulative_profit = 0
    
    for trade in trades:
        cumulative_profit += trade['Profit/Loss']
        performance.append({
            'Exit Time': trade['Exit Time'],
            'Cumulative Profit': cumulative_profit
        })
        
    return pd.DataFrame(performance)

def plot_performance(performance_df):
    if performance_df.empty:
        print("No trades to plot.")
        return
        
    plt.figure(figsize=(10, 5))
    plt.plot(performance_df['Exit Time'], performance_df['Cumulative Profit'], marker='o')
    plt.title('Cumulative Profit/Loss Over Time')
    plt.xlabel('Time')
    plt.ylabel('Cumulative Profit/Loss')
    plt.grid()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    conn = get_connection()
    if conn:
        call_data = fetch_call_options_data(conn)
        print(f"Total Call Options fetched: {len(call_data)}")
        
        trades = backtest_call_options(call_data)
        print("Summary of Trades:")
        for trade in trades:
            print(trade)
        
        # Calculate performance
        performance_df = calculate_performance(trades)
        print(performance_df)

        # Plot performance
        plot_performance(performance_df)

        conn.close()
