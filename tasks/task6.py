import bcsdbconfig
import pandas as pd
import matplotlib.pyplot as plt

# Get the connection
cn = bcsdbconfig.get_connection()

def check_available_options(cn):
    """Check available strike prices and option types."""
    query = """
    SELECT DISTINCT "Strike Price", "Option Type"
    FROM option_chain_tick_data;
    """
    df = pd.read_sql_query(query, cn)
    return df

def plot_option_ltp(cn, strike_price, option_type):
    """Plot the Last Traded Price (LTP) for a specific option."""
    query = f"""
    SELECT "Timestamp", "Bid Price" AS "LTP"
    FROM option_chain_tick_data
    WHERE "Strike Price" = {strike_price} AND "Option Type" = '{option_type}'
    ORDER BY "Timestamp";
    """
    
    df = pd.read_sql_query(query, cn)
    
    if df.empty:
        print(f"No data found for {option_type} with Strike Price {strike_price}.")
        return

    # Convert Timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(df['Timestamp'], df['LTP'], marker='o', linestyle='-')
    plt.title(f"Last Traded Price (LTP) for {option_type} {strike_price}")
    plt.xlabel('Timestamp')
    plt.ylabel('LTP')
    plt.xticks(rotation=45)
    plt.grid()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Check available options
    options_df = check_available_options(cn)
    print("Available Options:")
    print(options_df)

    # Specify a valid option to plot
    strike_price = 20000  # Update this to an available strike price
    option_type = 'Call'  # Update this to 'Call' or 'Put' based on your needs
    
    # Plot the option LTP
    plot_option_ltp(cn, strike_price, option_type)

    # Close connection
    cn.close()
