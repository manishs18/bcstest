import bcsdbconfig

def clean_option_chain_data(c):
    # Step 1: Identify and remove rows with missing or inconsistent data
    # Deleting rows with missing "Timestamp", "Strike Price", or "Volume"
    delete_query = """
    DELETE FROM option_chain_tick_data
    WHERE "Timestamp" IS NULL OR
          "Strike Price" IS NULL OR
          "Volume" IS NULL OR
          "Strike Price" < 0 OR
          "Volume" < 0;
    """
    c.execute(delete_query)
    
    # Step 2: Define and remove out-of-range values
    # Example criteria: Strike Price and Volume should be within realistic ranges
    # Adjust these ranges based on your data understanding
    delete_out_of_range_query = """
    DELETE FROM option_chain_tick_data
    WHERE "Strike Price" > 30000 OR  -- Adjust upper limit as necessary
          "Strike Price" < 0 OR
          "Volume" > 10000;          -- Adjust upper limit as necessary
    """
    c.execute(delete_out_of_range_query)

if __name__ == "__main__":
    cn = bcsdbconfig.get_connection()  # Establish the connection
    if cn:
        c = cn.cursor()  # Create a cursor
        
        # Clean the option chain data
        clean_option_chain_data(c)
        
        # Commit the changes
        cn.commit()
        print("Cleaned option_chain_tick_data: removed rows with missing or inconsistent data.")
        
        # Close the cursor and connection
        c.close()  # Close the cursor
        cn.close()  # Close the connection
