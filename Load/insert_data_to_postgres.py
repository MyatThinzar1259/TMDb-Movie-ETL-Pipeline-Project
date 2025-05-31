import os
import json
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from typing import List, Dict, Any

# Use shared db connection
from Load.db import get_connection

load_dotenv()
DATA_DIR = "Data/json_to_load"

def load_json(filename: str) -> List[Dict[str, Any]]:
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_data(conn, table: str, columns: List[str], data: List[Dict[str, Any]]) -> None:
    if not data:
        print(f"No data to insert into {table}")
        return
    with conn.cursor() as cur:
        query = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
            "ON CONFLICT DO NOTHING"
        )
        values = [tuple(d[col] for col in columns) for d in data]
        execute_values(cur, query, values)
        print(f"Inserted {len(data)} rows into {table}")

def run_etl_inserts(conn) -> None:
    table_defs = [
        ("movie", [
            "tmdb_id", "title", "budget", "revenue", "rating", "vote_count",
            "release_date", "original_language", "runtime", "source"
        ], "movies.json"),
        ("production_company", ["company_id", "name"], "production_companies.json"),
        ("genre", ["genre_id", "name"], "genres.json"),
        ("person", ["person_id", "name", "category"], "persons.json"),
        ("movie_person", ["tmdb_id", "person_id"], "movie_person.json"),
        ("movie_company", ["tmdb_id", "company_id"], "movie_company.json"),
        ("movie_genre", ["tmdb_id", "genre_id"], "movie_genre.json"),
    ]
    for table, columns, json_file in table_defs:
        insert_data(conn, table, columns, load_json(json_file))

def main() -> None:
    try:
        with get_connection() as conn:
            conn.autocommit = False
            run_etl_inserts(conn)
            conn.commit()
            print("All data inserted successfully.")
    except Exception as e:
        print("Error inserting data:", e)

if __name__ == "__main__":
    main()
