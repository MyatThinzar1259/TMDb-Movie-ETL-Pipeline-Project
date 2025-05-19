import os
import time
import logging
import requests
from dotenv import load_dotenv
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils_csv import save_movies_to_csv

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
MAX_RETRIES = 3
MAX_WORKERS = 50
FILTER_YEAR = 2024

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/movie_fetch.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def make_request_with_retries(url: str, params: dict, timeout: int = 20, retries: int = MAX_RETRIES, retry_delay: int = 3) -> Optional[dict]:
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Fetching URL: {url} (Attempt {attempt}/{retries})")
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on attempt {attempt}: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Max retries reached for URL: {url}")
                return None


def get_movie_details(movie_id: int) -> dict:
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {
        "api_key": API_KEY,
        "language": "en-US"
    }
    data = make_request_with_retries(url, params, retry_delay=3)
    if data is None:
        return {}
    return data


def extract_names(items: list, key: str) -> str:
    return ', '.join(item.get(key, '') for item in items if item.get(key))


def fetch_movies(language_code: str) -> List[Dict]:
    movies = []
    movie_ids = []
    page = 1
    total_pages = 1

    logger.info(f"Starting fetch for language: {language_code}")

    # Step 1: Collect all movie IDs
    while page <= total_pages:
        logger.info(f"Fetching page {page} of {total_pages} for language '{language_code}'...")
        url = f"{BASE_URL}/discover/movie"
        params = {
            'api_key': API_KEY,
            'language': 'en-US',
            'page': page,
            'with_original_language': language_code,
            'primary_release_date.gte': f'{FILTER_YEAR}-01-01',
            'primary_release_date.lte': f'{FILTER_YEAR}-12-31'
        }

        data = make_request_with_retries(url, params, retry_delay=5)
        if data is None:
            return movies

        if page == 1:
            total_pages = data.get('total_pages', 1)
            logger.info(f"Total pages to fetch for '{language_code}': {total_pages}")

        for movie in data.get('results', []):
            movie_id = movie.get('id')
            if movie_id:
                movie_ids.append((movie, movie_id))
        page += 1

    # Step 2: Fetch movie details in parallel
    def fetch_details(movie_tuple):
        movie, movie_id = movie_tuple
        details = get_movie_details(movie_id)
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
                logger.error(f"Exception occurred during detail fetch: {exc}")

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
    logger.info(f"Total time taken to fetch and save movies for '{language_code}': {total_time:.2f} seconds")


def main():
    languages = ['hi', 'ko', 'jp', 'th', 'tl']
    for lang in languages:
        logger.info("=" * 40)
        fetch_and_save_movies(lang)
        logger.info("=" * 40)


if __name__ == "__main__":
    main()