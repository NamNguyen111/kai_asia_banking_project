import psycopg2

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="db", 
            port=5432,
            dbname="db_banking",
            user="db_user_banking",
            password="db_password_banking"
        )
        print("Connected to PostgreSQL successfully.")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def connect_to_db_local(): # --> CÁI NÀY CHO KẾT NỐI TỪ LOCAL
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5001,
            dbname="db_banking",
            user="db_user_banking",
            password="db_password_banking"
        )
        print("Connected to PostgreSQL successfully.")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise