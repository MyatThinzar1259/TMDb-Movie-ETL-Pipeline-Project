import pandas as pd
import json
import os
from uuid import uuid4

# Directory containing your CSVs
CSV_DIR = "Data/clean_data"
OUTPUT_DIR = "Data/normalized_json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ID counters
company_id_counter = 1
genre_id_counter = 1
person_id_counter = 1

# Collectors for all tables
movies = []
production_companies = {}
genres = {}
persons = {}
movie_company_links = []
movie_genre_links = []
movie_person_links = []

LANGUAGES = ["en", "hi", "ja", "ko", "th"]  # example languages

def split_actors_field(actors_str):
    """
    Since actors in your CSV are space-separated without commas,
    use a heuristic: split by capitalized words sequences.
    This is a simple approach and may need tuning for edge cases.
    """
    import re
    # Matches sequences of capitalized words (names)
    pattern = r'([A-Z][a-z]+(?: [A-Z][a-z]+)*)'
    matches = re.findall(pattern, actors_str)
    return [m.strip() for m in matches if m.strip()]

for lang in LANGUAGES:
    csv_path = os.path.join(CSV_DIR, f"clean_{lang}_movies_2024.csv")
    if not os.path.exists(csv_path):
        print(f"Warning: {csv_path} not found, skipping.")
        continue

    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        tmdb_id = int(row["tmdb_id"])
        movie_entry = {
            "tmdb_id": tmdb_id,
            "title": row["title"],
            "budget": int(row.get("budget", 0)),
            "revenue": int(row.get("revenue", 0)),
            "rating": float(row.get("rating", 0)),
            "vote_count": int(row.get("vote_count", 0)),
            "release_date": row.get("release_date"),
            "original_language": row["original_language"],
            "runtime": float(row.get("runtime", 0)),
            "source": lang
        }
        movies.append(movie_entry)

        # Production companies (comma-separated)
        for name in str(row.get("production_companies", "")).split(","):
            name = name.strip()
            if name:
                if name not in production_companies:
                    production_companies[name] = company_id_counter
                    company_id_counter += 1
                movie_company_links.append({"tmdb_id": tmdb_id, "company_id": production_companies[name]})

        # Genres (comma-separated)
        for name in str(row.get("genres", "")).split(","):
            name = name.strip()
            if name:
                if name not in genres:
                    genres[name] = genre_id_counter
                    genre_id_counter += 1
                movie_genre_links.append({"tmdb_id": tmdb_id, "genre_id": genres[name]})

        # Directors (comma-separated)
        for name in str(row.get("directors", "")).split(","):
            name = name.strip()
            if name:
                if name not in persons:
                    persons[name] = {"id": person_id_counter, "category": "Director"}
                    person_id_counter += 1
                movie_person_links.append({"tmdb_id": tmdb_id, "person_id": persons[name]["id"]})

        # Actors (space-separated in your CSV, so use regex split)
        actors_raw = str(row.get("actors", ""))
        actors = split_actors_field(actors_raw)
        for name in actors:
            if name:
                if name not in persons:
                    persons[name] = {"id": person_id_counter, "category": "Actor"}
                    person_id_counter += 1
                movie_person_links.append({"tmdb_id": tmdb_id, "person_id": persons[name]["id"]})

def write_json(data, filename):
    with open(os.path.join(OUTPUT_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        write_json(movies, "movies.json")
write_json(
    [{"company_id": v, "name": k} for k, v in production_companies.items()],
    "production_companies.json"
)
write_json(
    [{"genre_id": v, "name": k} for k, v in genres.items()],
    "genres.json"
)
write_json(
    [{"person_id": v["id"], "name": k, "category": v["category"]} for k, v in persons.items()],
    "persons.json"
)
write_json(movie_company_links, "movie_company.json")
write_json(movie_genre_links, "movie_genre.json")
write_json(movie_person_links, "movie_person.json")

print("✅ Normalization and JSON export complete.")