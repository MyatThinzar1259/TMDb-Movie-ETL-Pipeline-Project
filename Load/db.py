import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in environment.")
    return psycopg2.connect(DATABASE_URL)

if __name__ == "__main__":
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                print("Database connection successful.")
    except Exception as e:
        print(f"Database connection failed: {e}")
