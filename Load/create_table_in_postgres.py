import os
from dotenv import load_dotenv
from db import get_connection

# Load DATABASE_URL from .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# SQL to create tables
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

def create_tables():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for sql in CREATE_TABLES_SQL:
                    cur.execute(sql)
            conn.commit()
            print("Tables created successfully.")
    except Exception as e:
        print("Error creating tables:", e)

if __name__ == "__main__":
    create_tables()
