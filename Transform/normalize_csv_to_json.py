import os
import glob
import json
import pandas as pd

# Paths
input_dir = 'Data/clean_data'
output_dir = 'Data/load_data'
os.makedirs(output_dir, exist_ok=True)

# Master sets and mappings to deduplicate and assign IDs
genre_id_map = {}
company_id_map = {}
person_id_map = {}

genre_counter = 1
company_counter = 1
person_counter = 1

# Final data holders
movies = []
genres = []
companies = []
persons = []

movie_genres = []
movie_companies = []
movie_persons = []

# Loop through all CSV files
for file in glob.glob(os.path.join(input_dir, "*.csv")):
    df = pd.read_csv(file)

    for _, row in df.iterrows():
        tmdb_id = row["tmdb_id"]
        title = row["title"]
        budget = row["budget"]
        revenue = row["revenue"]
        rating = row["rating"]
        vote_count = row["vote_count"]
        release_date = row["release_date"]
        original_language = row["original_language"]
        runtime = row["runtime"]
        source = row.get("source", os.path.basename(file))

        # --- Movie Table ---
        movies.append({
            "tmdb_id": tmdb_id,
            "title": title,
            "budget": budget,
            "revenue": revenue,
            "rating": rating,
            "vote_count": vote_count,
            "release_date": release_date,
            "original_language": original_language,
            "runtime": runtime,
            "source": source
        })

        # --- Genre Table + movie_genre ---
        genre_str = str(row.get("genres", ""))
        genre_list = [g.strip() for g in genre_str.split(",") if g.strip()]
        for genre in genre_list:
            if genre not in genre_id_map:
                genre_id_map[genre] = genre_counter
                genres.append({"genre_id": genre_counter, "name": genre})
                genre_counter += 1
            movie_genres.append({
                "tmdb_id": tmdb_id,
                "genre_id": genre_id_map[genre]
            })

        # --- Production Companies Table + movie_company ---
        company_str = str(row.get("production_companies", ""))
        company_list = [c.strip() for c in company_str.split(",") if c.strip()]
        for company in company_list:
            if company not in company_id_map:
                company_id_map[company] = company_counter
                companies.append({"company_id": company_counter, "name": company})
                company_counter += 1
            movie_companies.append({
                "tmdb_id": tmdb_id,
                "company_id": company_id_map[company]
            })

        # --- Person Table + movie_person ---
        # Directors separated by commas
        directors_str = str(row.get("directors", ""))
        directors_list = [p.strip() for p in directors_str.split(",") if p.strip()]
        for director in directors_list:
            if director not in person_id_map:
                person_id_map[director] = person_counter
                persons.append({
                    "person_id": person_counter,
                    "name": director,
                    "category": "Director"
                })
                person_counter += 1
            movie_persons.append({
                "tmdb_id": tmdb_id,
                "person_id": person_id_map[director]
            })

        # Actors separated by semicolons
        actors_str = str(row.get("actors", ""))
        actors_list = [p.strip() for p in actors_str.split(";") if p.strip()]
        for actor in actors_list:
            if actor not in person_id_map:
                person_id_map[actor] = person_counter
                persons.append({
                    "person_id": person_counter,
                    "name": actor,
                    "category": "Actor"
                })
                person_counter += 1
            movie_persons.append({
                "tmdb_id": tmdb_id,
                "person_id": person_id_map[actor]
            })

# Save function
def save_json(data, filename):
    with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# Save all tables to json files
save_json(movies, "movie.json")
save_json(companies, "production_company.json")
save_json(genres, "genre.json")
save_json(persons, "person.json")
save_json(movie_persons, "movie_person.json")
save_json(movie_companies, "movie_company.json")
save_json(movie_genres, "movie_genre.json")

print("✅ Normalization complete. JSON files saved to Data/load_data.")
