import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load DATABASE_URL from .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DATA_DIR = "Data/json_to_load"

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_data(conn, table, columns, data):
    if not data:
        print(f"No data to insert into {table}")
    with conn.cursor() as cur:
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        values = [tuple(d[col] for col in columns) for d in data]
        execute_values(cur, query, values)
        print(f"Inserted {len(data)} rows into {table}")

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.autocommit = False

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
        print("All data inserted successfully.")
    except Exception as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()