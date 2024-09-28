import bcsdbconfig

def filter_options(c):
    query = """
    SELECT *
    FROM option_chain_tick_data
    WHERE "Strike Price" > 15000 AND "Volume" > 1000
    LIMIT 5;
    """
    c.execute(query)
    rows = c.fetchall()
    return rows

def count_options(c):
    count_query = """
    SELECT COUNT(*)
    FROM option_chain_tick_data
    WHERE "Strike Price" > 15000 AND "Volume" > 1000;
    """
    c.execute(count_query)
    return c.fetchone()[0]

if __name__ == "__main__":
    cn = bcsdbconfig.get_connection()
    if cn:
        c = cn.cursor()
        
        # Count options meeting criteria
        total_count = count_options(c)
        print(f"Total options meeting criteria: {total_count}")

        # Filter the options
        filtered_options = filter_options(c)
        
        print("Filtered Options (Strike Price > 15000 and Volume > 1000):")
        for row in filtered_options:
            print(row)

        c.close()
        cn.close()
