import bcsdbconfig
import pandas as pd
import matplotlib.pyplot as plt

# Get the connection
cn = bcsdbconfig.get_connection()

def get_last_expiry(cn):
    """Retrieve the last expiry date from option_chain_tick_data."""
    query = """
    SELECT DISTINCT "Timestamp"::date AS expiry_date
    FROM option_chain_tick_data
    ORDER BY expiry_date DESC
    LIMIT 1;
    """
    result = pd.read_sql_query(query, cn)
    return result.iloc[0]['expiry_date'] if not result.empty else None

def plot_open_interest(cn, expiry_date):
    """Plot the open interest for Call and Put options over time for the last expiry."""
    query = f"""
    SELECT "Timestamp", "Open Interest", "Option Type"
    FROM option_chain_tick_data
    WHERE "Timestamp"::date = '{expiry_date}';
    """
    
    df = pd.read_sql_query(query, cn)
    
    if df.empty:
        print(f"No data found for expiry date {expiry_date}.")
        return

    # Convert Timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Separate data for Calls and Puts
    calls = df[df['Option Type'] == 'Call']
    puts = df[df['Option Type'] == 'Put']

    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(calls['Timestamp'], calls['Open Interest'], marker='o', linestyle='-', label='Call OI', color='blue')
    plt.plot(puts['Timestamp'], puts['Open Interest'], marker='o', linestyle='-', label='Put OI', color='red')
    
    plt.title(f"Open Interest (OI) for Calls and Puts on {expiry_date}")
    plt.xlabel('Timestamp')
    plt.ylabel('Open Interest')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Get the last expiry date
    last_expiry = get_last_expiry(cn)
    if last_expiry:
        print(f"Last Expiry Date: {last_expiry}")
        # Plot open interest for the last expiry
        plot_open_interest(cn, last_expiry)
    else:
        print("No expiry dates found.")

    # Close connection
    cn.close()
