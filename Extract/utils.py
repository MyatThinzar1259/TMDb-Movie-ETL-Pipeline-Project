import csv
import os
import re
from typing import List, Dict

def load_movies_from_csv(input_path: str) -> List[Dict]:
    """Load movies from a CSV file into a list of dicts."""
    if not os.path.exists(input_path):
        return []
    with open(input_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row for row in reader]

def compare_movie_records(new: Dict, old: Dict, keys: List[str]) -> bool:
    """Return True if any of the specified keys differ between new and old."""
    for key in keys:
        if str(new.get(key, "")).strip() != str(old.get(key, "")).strip():
            return True
    return False

def save_movies_to_csv(
    movies: List[Dict],
    output_path: str,
    append: bool = False
) -> bool:
    """
    Save a list of dictionaries to a CSV file.
    Returns True if successful, False otherwise.
    """
    if not movies:
        print(f"[WARNING] No data found to save at {output_path}.")
        return False
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    mode = 'a' if append else 'w'
    try:
        fieldnames = [
            'tmdb_id', 'title', 'budget', 'revenue', 'rating', 'vote_count',
            'release_date', 'original_language', 'production_companies',
            'genres', 'directors', 'actors', 'runtime', 'is_data_updated'
        ]

        with open(output_path, mode, encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not append or (append and f.tell() == 0):
                writer.writeheader()
            writer.writerows(movies)
        print(f"[SUCCESS] Saved {len(movies)} records to {output_path}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to save CSV: {e}")
        return False

def extract_names(items: list, key: str) -> str:
    return ', '.join(item.get(key, '') for item in items if item.get(key))

def format_actors(actors: list) -> str:
    return ', '.join([f"{a['name']} ({a.get('character', '')})" for a in actors if a.get('name')])

def normalize_title(s: str) -> str:
    """Normalize a movie title for comparison: lowercase, remove punctuation, extra spaces."""
    return re.sub(r'[\W_]+', ' ', s or '').strip().lower()
