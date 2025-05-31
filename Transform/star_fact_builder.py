import pandas as pd
import json
import os

class StarFactBuilder:
    def __init__(self, csv_dir="Data/clean_data", json_dir="Data/json_to_load", output_path="Data/star_json/fact.json"):
        self.csv_dir = csv_dir
        self.json_dir = json_dir
        self.output_path = output_path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    def load_json_as_dict(self, filename, key_field, value_field):
        with open(os.path.join(self.json_dir, filename), encoding="utf-8") as f:
            data = json.load(f)
        return {item[key_field]: item[value_field] for item in data}

    def load_bridge(self, filename, key="fact_id", value="*_id"):
        with open(os.path.join(self.json_dir, filename), encoding="utf-8") as f:
            data = json.load(f)
        result = {}
        for row in data:
            fid = row[key]
            vid = list(row.values())[1]
            result.setdefault(fid, []).append(vid)
        return result

    def merge_fact_table(self):
        # Load base fact data from CSVs
        all_csvs = [os.path.join(self.csv_dir, f) for f in os.listdir(self.csv_dir) if f.endswith(".csv")]
        df = pd.concat([pd.read_csv(f) for f in all_csvs], ignore_index=True)
        df.insert(0, "fact_id", range(1, len(df) + 1))

        # Load dimension mappings
        companies = self.load_json_as_dict("production_company.json", "company_id", "name")
        genres = self.load_json_as_dict("genre.json", "genre_id", "name")
        directors = self.load_json_as_dict("director.json", "director_id", "name")
        actors = self.load_json_as_dict("actor.json", "actor_id", "name")

        # Load bridge tables
        fact_companies = self.load_bridge("fact_company.json")
        fact_genres = self.load_bridge("fact_genre.json")
        fact_directors = self.load_bridge("fact_director.json")
        fact_actors = self.load_bridge("fact_actor.json")

        # Helper function to map IDs to names
        def map_ids(ids, lookup):
            return [lookup.get(i, f"Unknown({i})") for i in ids]

        enriched = []
        for _, row in df.iterrows():
            fid = row["fact_id"]
            row_dict = row.to_dict()

            # Remove original string columns to avoid duplicates
            for col in ["production_companies", "genres", "directors", "actors"]:
                if col in row_dict:
                    del row_dict[col]

            # Add only the enriched list columns with duplicates removed
            row_dict.update({
                "production_companies": list(set(map_ids(fact_companies.get(fid, []), companies))),
                "genres": list(set(map_ids(fact_genres.get(fid, []), genres))),
                "directors": list(set(map_ids(fact_directors.get(fid, []), directors))),
                "actors": list(set(map_ids(fact_actors.get(fid, []), actors))),
            })

            enriched.append(row_dict)

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, indent=2, ensure_ascii=False)

        print(f"✅ Enriched fact.json saved to: {self.output_path}")

def main():
    builder = StarFactBuilder()
    builder.merge_fact_table()

if __name__ == "__main__":
    main()
