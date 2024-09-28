import bcsdbconfig

def get_option_type_volume(c):
    query = """
    SELECT "Option Type", SUM("Volume") AS total_volume
    FROM option_chain_tick_data
    GROUP BY "Option Type";
    """
    c.execute(query)
    return c.fetchall()

if __name__ == "__main__":
    cn = bcsdbconfig.get_connection()  # Establish the connection
    if cn:
        c = cn.cursor()  # Create a cursor
        
        # Calculate total volume for each option type
        option_volumes = get_option_type_volume(c)
        
        print("\nTotal Volume for Each Option Type:")
        for option_type, total_volume in option_volumes:
            print(f"{option_type}: Total Volume = {total_volume}")

        # Close the cursor and connection
        c.close()  # Close the cursor
        cn.close()  # Close the connection
