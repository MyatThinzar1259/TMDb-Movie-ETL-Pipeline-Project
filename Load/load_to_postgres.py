import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()  # Load .env variables

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)

DATA_DIR = "Data/load_data"  # where your JSON files are

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_data(conn, table, columns, data):
    if not data:
        print(f"No data to insert into {table}")
        return
    with conn.cursor() as cur:
        # Prepare the insert query, e.g. INSERT INTO table (col1, col2) VALUES %s
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        values = [tuple(d[col] for col in columns) for d in data]
        execute_values(cur, query, values)
        print(f"Inserted {len(data)} rows into {table}")

def main():
    conn = connect_db()
    try:
        # Disable autocommit for transaction
        conn.autocommit = False

        # 1. movie
        movie_data = load_json("movie.json")
        insert_data(conn, "movie",
                    ["tmdb_id", "title", "budget", "revenue", "rating", "vote_count", "release_date",
                     "original_language", "runtime", "source"],
                    movie_data)

        # 2. production_company
        company_data = load_json("production_company.json")
        insert_data(conn, "production_company", ["company_id", "name"], company_data)

        # 3. genre
        genre_data = load_json("genre.json")
        insert_data(conn, "genre", ["genre_id", "name"], genre_data)

        # 4. person
        person_data = load_json("person.json")
        insert_data(conn, "person", ["person_id", "name", "category"], person_data)

        # 5. movie_person
        movie_person_data = load_json("movie_person.json")
        insert_data(conn, "movie_person", ["tmdb_id", "person_id"], movie_person_data)

        # 6. movie_company
        movie_company_data = load_json("movie_company.json")
        insert_data(conn, "movie_company", ["tmdb_id", "company_id"], movie_company_data)

        # 7. movie_genre
        movie_genre_data = load_json("movie_genre.json")
        insert_data(conn, "movie_genre", ["tmdb_id", "genre_id"], movie_genre_data)

        conn.commit()
        print("✅ All data loaded successfully!")
    except Exception as e:
        conn.rollback()
        print("❌ Error loading data:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
