import psycopg2

database = "bcs"
user = "postgres" 
password = "password"
host = "localhost"
port = "5432"  # Default PostgreSQL port

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

if __name__ == "__main__":
    connection = get_connection()
    if connection:
        connection.close()