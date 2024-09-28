import bcsdbconfig

cn = bcsdbconfig.get_connection()
c = cn.cursor()

def calculate_statistics(c, table_name, columns):
    stats = {}
    for column in columns:
        # Use double quotes for all columns to handle case sensitivity
        column_name = f'"{column}"'
        
        query = f"""
        SELECT 
            AVG({column_name}) AS mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column_name}) AS median,
            MIN({column_name}) AS min,
            MAX({column_name}) AS max
        FROM {table_name};
        """
        c.execute(query)
        stats[column] = c.fetchone()
    return stats

if __name__ == "__main__":
    if cn:
        # statistics for Nifty Tick Data
        nifty_columns = ['Open', 'Close', 'Volume']
        nifty_stats = calculate_statistics(c, 'nifty_tick_data', nifty_columns)
        print("\nNifty Tick Data Statistics:")
        for column, values in nifty_stats.items():
            print(f"{column}: Mean={values[0]}, Median={values[1]}, Min={values[2]}, Max={values[3]}")

        # statistics for Option Chain Data
        option_columns = ['Strike Price', 'Volume', 'Open Interest']
        option_stats = calculate_statistics(c, 'option_chain_tick_data', option_columns)
        print("\nOption Chain Data Statistics:")
        for column, values in option_stats.items():
            print(f"{column}: Mean={values[0]}, Median={values[1]}, Min={values[2]}, Max={values[3]}")

        # Close the cursor and the connection
        c.close()
        cn.close()
