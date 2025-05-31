import pandas as pd
import json
import os
import re
from datetime import datetime

class DataNormalizer:
    def __init__(self, csv_dir="Data/clean_data", output_dir="Data/json_to_load"):
        self.csv_dir = csv_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.company_id_counter = 1
        self.genre_id_counter = 1
        self.director_id_counter = 1
        self.actor_id_counter = 1
        self.fact_id_counter = 1

        self.production_companies = {}
        self.genres = {}
        self.directors = {}
        self.actors = {}
        self.date_dim = {}
        self.fact_id_map = {}

        self.movies = []
        self.fact_movie = []
        self.fact_companies = []
        self.fact_genres = []
        self.fact_directors = []
        self.fact_actors = []

    def _split_actors_field(self, actors_str):
        pattern = r'([A-Z][a-z]+(?: [A-Z][a-z]+)*)'
        matches = re.findall(pattern, actors_str)
        return [m.strip() for m in matches if m.strip()]

    def _write_json(self, data, filename):
        path = os.path.join(self.output_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def process_files(self):
        for filename in os.listdir(self.csv_dir):
            if not filename.endswith(".csv"):
                continue

            csv_path = os.path.join(self.csv_dir, filename)
            df = pd.read_csv(csv_path)

            for _, row in df.iterrows():
                tmdb_id = int(row["tmdb_id"])
                fact_id = self.fact_id_counter
                self.fact_id_map[tmdb_id] = fact_id
                self.fact_id_counter += 1

                self.movies.append({
                    "tmdb_id": tmdb_id,
                    "title": row["title"]
                })

                release_date = row.get("release_date", "")
                if release_date and release_date not in self.date_dim:
                    try:
                        date_obj = datetime.strptime(release_date, "%Y-%m-%d")
                        self.date_dim[release_date] = {
                            "release_date": release_date,
                            "year": date_obj.year,
                            "month": date_obj.month,
                            "day": date_obj.day
                        }
                    except Exception as e:
                        print(f"Invalid date format: {release_date} in file {filename}")

                self.fact_movie.append({
                    "fact_id": fact_id,
                    "movie_id": tmdb_id
                })

                for name in str(row.get("production_companies", "")).split(","):
                    name = name.strip()
                    if name:
                        if name not in self.production_companies:
                            self.production_companies[name] = self.company_id_counter
                            self.company_id_counter += 1
                        self.fact_companies.append({
                            "fact_id": fact_id,
                            "company_id": self.production_companies[name]
                        })

                for name in str(row.get("genres", "")).split(","):
                    name = name.strip()
                    if name:
                        if name not in self.genres:
                            self.genres[name] = self.genre_id_counter
                            self.genre_id_counter += 1
                        self.fact_genres.append({
                            "fact_id": fact_id,
                            "genre_id": self.genres[name]
                        })

                for name in str(row.get("directors", "")).split(","):
                    name = name.strip()
                    if name:
                        if name not in self.directors:
                            self.directors[name] = self.director_id_counter
                            self.director_id_counter += 1
                        self.fact_directors.append({
                            "fact_id": fact_id,
                            "director_id": self.directors[name]
                        })

                actors_raw = str(row.get("actors", ""))
                actors = self._split_actors_field(actors_raw)
                for name in actors:
                    if name:
                        if name not in self.actors:
                            self.actors[name] = self.actor_id_counter
                            self.actor_id_counter += 1
                        self.fact_actors.append({
                            "fact_id": fact_id,
                            "actor_id": self.actors[name]
                        })

    def export_to_json(self):
        self._write_json(self.movies, "movie.json")
        self._write_json(
            [{"company_id": v, "name": k} for k, v in self.production_companies.items()],
            "production_company.json"
        )
        self._write_json(
            [{"genre_id": v, "name": k} for k, v in self.genres.items()],
            "genre.json"
        )
        self._write_json(
            [{"director_id": v, "name": k} for k, v in self.directors.items()],
            "director.json"
        )
        self._write_json(
            [{"actor_id": v, "name": k} for k, v in self.actors.items()],
            "actor.json"
        )
        self._write_json(list(self.date_dim.values()), "date.json")
        self._write_json(self.fact_movie, "fact_movie.json")
        self._write_json(self.fact_companies, "fact_company.json")
        self._write_json(self.fact_genres, "fact_genre.json")
        self._write_json(self.fact_directors, "fact_director.json")
        self._write_json(self.fact_actors, "fact_actor.json")

def main():
    normalizer = DataNormalizer()
    normalizer.process_files()
    normalizer.export_to_json()

if __name__ == "__main__":
    main()
