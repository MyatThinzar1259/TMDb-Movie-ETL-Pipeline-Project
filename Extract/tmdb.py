import os
import time
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from api_client import TMDbAPIClient
from utils import save_movies_to_csv, extract_names, format_actors

# Constants
FILTER_YEAR = 2024
MAX_PAGES = 10
MAX_WORKERS = 50

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/tmdb_movie_fetch.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TMDbMovieFetcher:
    """Handles fetching movies from TMDb discover API"""

    def __init__(self):
        self.api_client = TMDbAPIClient()

    def discover_movies_by_language(self, language_code: str) -> List[tuple]:
        """Discover movies by language and return list of (movie, movie_id) tuples"""
        movie_ids = []
        page = 1
        total_pages = 1

        logger.info(f"Starting discovery for language: {language_code}")

        while page <= total_pages and page <= MAX_PAGES:
            logger.info(f"Fetching page {page} of {total_pages} for language '{language_code}'...")

            url = f"{self.api_client.base_url}/discover/movie"
            params = {
                'api_key': self.api_client.api_key,
                'language': 'en-US',
                'page': page,
                'with_original_language': language_code,
                'primary_release_date.gte': f'{FILTER_YEAR}-01-01',
                'primary_release_date.lte': f'{FILTER_YEAR}-12-31'
            }

            data = self.api_client.make_request_with_retries(url, params, retry_delay=5)
            if data is None:
                break

            if page == 1:
                total_pages = data.get('total_pages', 1)
                logger.info(f"Total pages to fetch for '{language_code}': {total_pages}")

            for movie in data.get('results', []):
                movie_id = movie.get('id')
                if movie_id:
                    movie_ids.append((movie, movie_id))
            page += 1

        return movie_ids

    def process_movie_details(self, movie_tuple: tuple) -> Dict:
        """Process individual movie to get full details"""
        movie, movie_id = movie_tuple
        details = self.api_client.get_movie_full_details(movie_id)

        production_companies = extract_names(details.get('production_companies', []), 'name')
        genres = extract_names(details.get('genres', []), 'name')
        actors = format_actors(details.get('actors',[]))

        return {
            'tmdb_id': movie_id,
            'title': movie.get('title'),
            'budget': details.get('budget'),
            'revenue': details.get('revenue'),
            'rating': movie.get('vote_average'),
            'vote_count': movie.get('vote_count'),
            'release_date': movie.get('release_date'),
            'original_language': movie.get('original_language'),
            'production_companies': production_companies,
            'genres': genres,
            'directors': ', '.join(details.get('directors', [])),
            'actors': actors,
            'runtime': details.get('runtime')
        }

    def fetch_movies(self, language_code: str) -> List[Dict]:
        """Main method to fetch and process movies"""
        movie_ids = self.discover_movies_by_language(language_code)
        movies = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.process_movie_details, tup) for tup in movie_ids]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    movies.append(result)
                except Exception as exc:
                    logger.error(f"Exception occurred during detail fetch: {exc}")

        return movies

def fetch_and_save_movies(
    language_code: str,
    output_file: Optional[str] = None,
    output_dir: str = "Data/raw_data"
) -> None:
    """Fetch movies and save to CSV"""
    start_time = time.time()

    if not output_file:
        output_file = f"{language_code}_movies_{FILTER_YEAR}.csv"
    output_path = os.path.join(output_dir, output_file)

    fetcher = TMDbMovieFetcher()
    movies = fetcher.fetch_movies(language_code)
    save_movies_to_csv(movies, output_path)

    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Total time taken to fetch and save movies for '{language_code}': {total_time:.2f} seconds")

def main():
    """Main function"""
    languages = ['hi', 'ko', 'jp', 'th', 'tl']  # Add more languages as needed: ['hi', 'ko', 'jp', 'th', 'tl']

    for lang in languages:
        logger.info("=" * 40)
        fetch_and_save_movies(lang)
        logger.info("=" * 40)

if __name__ == "__main__":
    main()
