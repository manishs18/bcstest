import bcsdbconfig

def format_timestamps(c):
    # Step 1: Update Timestamp to ensure it's in the correct date-time format
    update_query = """
    UPDATE option_chain_tick_data
    SET "Timestamp" = "Timestamp"::timestamp
    WHERE "Timestamp" IS NOT NULL;
    """
    c.execute(update_query)

    # Step 2: Ensure chronological sorting (if needed)
    # This step assumes you're storing and querying in a chronological order.
    # If the database schema allows, you may want to reorder or create an index.
    # In this case, you can create an index to optimize queries:
    create_index_query = """
    CREATE INDEX IF NOT EXISTS idx_timestamp ON option_chain_tick_data("Timestamp");
    """
    c.execute(create_index_query)

if __name__ == "__main__":
    cn = bcsdbconfig.get_connection()  # Establish the connection
    if cn:
        c = cn.cursor()  # Create a cursor
        
        # Format the timestamps
        format_timestamps(c)
        
        # Commit the changes
        cn.commit()
        print("Formatted Timestamp in option_chain_tick_data.")

        # Close the cursor and connection
        c.close()  # Close the cursor
        cn.close()  # Close the connection
