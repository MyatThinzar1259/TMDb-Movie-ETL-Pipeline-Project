import requests
import os
from dotenv import load_dotenv
import time
from typing import List, Dict, Optional
from utils_csv import save_movies_to_csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
MAX_RETRIES = 3
MAX_WORKERS = 50 # Adjust this based on your system's capabilities 50 to 100
FILTER_YEAR = 2024

def get_movie_details(movie_id: int, max_retries: int = MAX_RETRIES) -> dict:
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

def fetch_movies(language_code: str, max_retries: int = MAX_RETRIES) -> List[Dict]:
    movies = []
    movie_ids = []
    page = 1
    total_pages = 1

    print(f"[INFO] Starting fetch for language: {language_code}")

    # Step 1: Collect all movie IDs
    while page <= total_pages:
        print(f"[INFO] Fetching page {page} of {total_pages} for language '{language_code}'...")
        url = f"{BASE_URL}/discover/movie"
        params = {
            'api_key': API_KEY,
            'language': 'en-US',
            'page': page,
            'with_original_language': language_code,
            'primary_release_date.gte': f'{FILTER_YEAR}-01-01',
            'primary_release_date.lte': f'{FILTER_YEAR}-12-31'
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
            if movie_id:
                movie_ids.append((movie, movie_id))
        page += 1

    # Step 2: Fetch movie details in parallel
    def fetch_details(movie_tuple):
        movie, movie_id = movie_tuple

        details = get_movie_details(movie_id, max_retries=max_retries)

        production_companies = extract_names(details.get('production_companies', []), 'name')
        genres = extract_names(details.get('genres', []), 'name')
        return {
            'title': movie.get('title'),
            'rating': movie.get('vote_average'),
            'release_date': movie.get('release_date'),
            'original_language': movie.get('original_language'),
            'production_companies': production_companies,
            'genres': genres
        }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        for tup in movie_ids:
            future = executor.submit(fetch_details, tup)
            futures.append(future)

        for future in as_completed(futures):
            try:
                result = future.result()
                movies.append(result)
            except Exception as exc:
                print(f"[ERROR] Exception occurred: {exc}")

    return movies

def fetch_and_save_movies(language_code: str, output_file: Optional[str] = None, output_dir: str = "raw_data") -> None:
    start_time = time.time()
    if not output_file:
        output_file = f"{language_code}_movies_{FILTER_YEAR}.csv"
    output_path = os.path.join(output_dir, output_file)
    movies = fetch_movies(language_code)
    save_movies_to_csv(movies, output_path)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"[INFO] Total time taken to fetch and save movies for '{language_code}': {total_time:.2f} seconds")

def main():
    languages = ['hi', 'ko', 'jp', 'th', 'tl']
    for lang in languages:
        print("="*40)
        fetch_and_save_movies(lang)
        print("="*40)

if __name__ == "__main__":
    main()
