import csv
import os
from typing import List, Dict

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
        with open(output_path, mode, encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=movies[0].keys())
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
    return '; '.join([f"{a['name']} ({a.get('character', '')})" for a in actors if a.get('name')])
