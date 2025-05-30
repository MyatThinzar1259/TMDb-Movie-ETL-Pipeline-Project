import pandas as pd
import json
import os
import re
class DataNormalizer:
    def __init__(self, csv_dir="Data/clean_data", output_dir="Data/json_to_load"):
        self.csv_dir = csv_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # ID counters
        self.company_id_counter = 1
        self.genre_id_counter = 1
        self.person_id_counter = 1

        # Collectors for all tables
        self.movies = []
        self.production_companies = {}
        self.genres = {}
        self.persons = {}
        self.movie_company_links = []
        self.movie_genre_links = []
        self.movie_person_links = []

    def _split_actors_field(self, actors_str):
        """
        Since actors in your CSV are space-separated without commas,
        use a heuristic: split by capitalized words sequences.
        This is a simple approach and may need tuning for edge cases.
        """
        # Matches sequences of capitalized words (names)
        pattern = r'([A-Z][a-z]+(?: [A-Z][a-z]+)*)'
        matches = re.findall(pattern, actors_str)
        return [m.strip() for m in matches if m.strip()]

    def _write_json(self, data, filename):
        with open(os.path.join(self.output_dir, filename), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def process_files(self):
        # Process all CSV files in the directory
        for filename in os.listdir(self.csv_dir):
            if not filename.endswith(".csv"):
                continue
            csv_path = os.path.join(self.csv_dir, filename)
            print(f"Processing file: {csv_path}")
            df = pd.read_csv(csv_path)

            # Try to infer language from filename, fallback to 'unknown'
            lang = filename.split("_")[1] if "_" in filename else "unknown"

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
                    "original_language": row.get("original_language", lang),
                    "runtime": float(row.get("runtime", 0)),
                    "source": lang
                }
                self.movies.append(movie_entry)

                # Production companies (comma-separated)
                for name in str(row.get("production_companies", "")).split(","):
                    name = name.strip()
                    if name:
                        if name not in self.production_companies:
                            self.production_companies[name] = self.company_id_counter
                            self.company_id_counter += 1
                        self.movie_company_links.append({"tmdb_id": tmdb_id, "company_id": self.production_companies[name]})

                # Genres (comma-separated)
                for name in str(row.get("genres", "")).split(","):
                    name = name.strip()
                    if name:
                        if name not in self.genres:
                            self.genres[name] = self.genre_id_counter
                            self.genre_id_counter += 1
                        self.movie_genre_links.append({"tmdb_id": tmdb_id, "genre_id": self.genres[name]})

                # Directors (comma-separated)
                for name in str(row.get("directors", "")).split(","):
                    name = name.strip()
                    if name:
                        if name not in self.persons:
                            self.persons[name] = {"id": self.person_id_counter, "category": "Director"}
                            self.person_id_counter += 1
                        self.movie_person_links.append({"tmdb_id": tmdb_id, "person_id": self.persons[name]["id"]})

                # Actors (space-separated in your CSV, so use regex split)
                actors_raw = str(row.get("actors", ""))
                actors = self._split_actors_field(actors_raw)
                for name in actors:
                    if name:
                        if name not in self.persons:
                            self.persons[name] = {"id": self.person_id_counter, "category": "Actor"}
                            self.person_id_counter += 1
                        self.movie_person_links.append({"tmdb_id": tmdb_id, "person_id": self.persons[name]["id"]})
        print("File processing complete.")

    def export_to_json(self):
        self._write_json(self.movies, "movies.json")
        self._write_json(
            [{"company_id": v, "name": k} for k, v in self.production_companies.items()],
            "production_companies.json"
        )
        self._write_json(
            [{"genre_id": v, "name": k} for k, v in self.genres.items()],
            "genres.json"
        )
        self._write_json(
            [{"person_id": v["id"], "name": k, "category": v["category"]} for k, v in self.persons.items()],
            "persons.json"
        )
        self._write_json(self.movie_company_links, "movie_company.json")
        self._write_json(self.movie_genre_links, "movie_genre.json")
        self._write_json(self.movie_person_links, "movie_person.json")
        print("Normalization and JSON export complete.")

def main():
    """Main function to run the data normalization and export."""
    normalizer = DataNormalizer()
    normalizer.process_files()
    normalizer.export_to_json()

if __name__ == "__main__":
    main()
