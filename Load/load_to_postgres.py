import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load DATABASE_URL from .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

DATA_DIR = "Data/json_to_load"

# Define SQL to create tables
CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS movie (
        tmdb_id INTEGER PRIMARY KEY,
        title TEXT,
        budget INTEGER,
        revenue INTEGER,
        rating FLOAT,
        vote_count INTEGER,
        release_date DATE,
        original_language TEXT,
        runtime FLOAT,
        source TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS production_company (
        company_id TEXT PRIMARY KEY,
        name TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS genre (
        genre_id TEXT PRIMARY KEY,
        name TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS person (
        person_id TEXT PRIMARY KEY,
        name TEXT,
        category TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS movie_person (
        tmdb_id INTEGER REFERENCES movie(tmdb_id),
        person_id TEXT REFERENCES person(person_id),
        PRIMARY KEY (tmdb_id, person_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS movie_company (
        tmdb_id INTEGER REFERENCES movie(tmdb_id),
        company_id TEXT REFERENCES production_company(company_id),
        PRIMARY KEY (tmdb_id, company_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS movie_genre (
        tmdb_id INTEGER REFERENCES movie(tmdb_id),
        genre_id TEXT REFERENCES genre(genre_id),
        PRIMARY KEY (tmdb_id, genre_id)
    );
    """
]

def connect_db():
    return psycopg2.connect(DATABASE_URL)

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_data(conn, table, columns, data):
    if not data:
        print(f"No data to insert into {table}")
        return
    with conn.cursor() as cur:
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        values = [tuple(d[col] for col in columns) for d in data]
        execute_values(cur, query, values)
        print(f"Inserted {len(data)} rows into {table}")

def create_tables(conn):
    with conn.cursor() as cur:
        for sql in CREATE_TABLES_SQL:
            cur.execute(sql)
        print("All tables created or already exist.")

def main():
    conn = connect_db()
    try:
        conn.autocommit = False

        # Step 1: Create tables
        create_tables(conn)

        # Step 2: Load and insert data
        insert_data(conn, "movie", [
            "tmdb_id", "title", "budget", "revenue", "rating", "vote_count",
            "release_date", "original_language", "runtime", "source"
        ], load_json("movies.json"))

        insert_data(conn, "production_company", ["company_id", "name"], load_json("production_companies.json"))
        insert_data(conn, "genre", ["genre_id", "name"], load_json("genres.json"))
        insert_data(conn, "person", ["person_id", "name", "category"], load_json("persons.json"))
        insert_data(conn, "movie_person", ["tmdb_id", "person_id"], load_json("movie_person.json"))
        insert_data(conn, "movie_company", ["tmdb_id", "company_id"], load_json("movie_company.json"))
        insert_data(conn, "movie_genre", ["tmdb_id", "genre_id"], load_json("movie_genre.json"))

        conn.commit()
        print("All data loaded successfully!")
    except Exception as e:
        conn.rollback()
        print("Error loading data:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()