import requests
import os
from dotenv import load_dotenv
import time
from typing import List, Dict, Optional
from utils_csv import save_movies_to_csv

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

def get_movie_details(movie_id: int, max_retries: int = 3) -> dict:
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {
        "api_key": API_KEY,
        "language": "en-US"
    }
    retries = 0
    while retries < max_retries:
        try:
            print(f"[INFO] Fetching details for movie ID: {movie_id} (Attempt {retries+1}/{max_retries})")
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            print(f"[INFO] Successfully fetched details for movie ID: {movie_id}")
            return resp.json()
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"[ERROR] Failed to fetch details for movie {movie_id}: {e}")
            if retries < max_retries:
                print(f"[INFO] Retrying in 3 seconds...")
                time.sleep(3)
            else:
                print(f"[ERROR] Max retries reached for movie {movie_id}. Skipping.")
                return {}
    return {}

def extract_names(items: list, key: str) -> str:
    return ', '.join(item.get(key, '') for item in items if item.get(key))

def fetch_movies(language_code: str, year: int = 2024, max_retries: int = 3) -> List[Dict]:
    movies = []
    page = 1
    total_pages = 1

    print(f"[INFO] Starting fetch for language: {language_code}")

    while page <= total_pages:
        print(f"[INFO] Fetching page {page} of {total_pages} for language '{language_code}'...")
        url = f"{BASE_URL}/discover/movie"
        params = {
            'api_key': API_KEY,
            'language': 'en-US',
            'page': page,
            'with_original_language': language_code,
            'primary_release_date.gte': f'{year}-01-01',
            'primary_release_date.lte': f'{year}-12-31'
        }
        retries = 0
        while retries < max_retries:
            try:
                resp = requests.get(url, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"[ERROR] Failed to fetch page {page} for '{language_code}': {e}")
                if retries < max_retries:
                    print(f"[INFO] Retrying in 5 seconds... (Attempt {retries}/{max_retries})")
                    time.sleep(5)
                else:
                    print(f"[ERROR] Max retries reached for page {page}. Skipping.")
                    return movies

        if page == 1:
            total_pages = data.get('total_pages', 1)
            print(f"[INFO] Total pages to fetch for '{language_code}': {total_pages}")
        for movie in data.get('results', []):
            movie_id = movie.get('id')
            details = get_movie_details(movie_id, max_retries=max_retries) if movie_id else {}
            production_companies = extract_names(details.get('production_companies', []), 'name')
            genres = extract_names(details.get('genres', []), 'name')
            movies.append({
                'title': movie.get('title'),
                'overview': movie.get('overview'),
                'rating': movie.get('vote_average'),
                'release_date': movie.get('release_date'),
                'original_language': movie.get('original_language'),
                'production_companies': production_companies,
                'genres': genres
            })
        page += 1
    return movies

def fetch_and_save_movies(language_code: str, output_file: Optional[str] = None, output_dir: str = "results") -> None:
    if not output_file:
        output_file = f"{language_code}_movies_simple.csv"
    output_path = os.path.join(output_dir, output_file)
    movies = fetch_movies(language_code)
    save_movies_to_csv(movies, output_path)

def main():
    languages = ['hi']
    for lang in languages:
        print("="*40)
        fetch_and_save_movies(lang)
        print("="*40)

if __name__ == "__main__":
    main()
