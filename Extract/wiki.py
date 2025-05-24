import os
import time
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from api_client import TMDbAPIClient
from utils_csv import save_movies_to_csv
from utils_tmdb import extract_names, format_actors

# Constants
WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_American_films_of_2024'
OUTPUT_DIR = "raw_data"
OUTPUT_FILE = "american_movies_2024.csv"
MAX_WORKERS = 20

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/wiki_movie_fetch.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WikipediaMovieScraper:
    """Handles scraping movies from Wikipedia and enriching with TMDb data"""

    def __init__(self):
        self.api_client = TMDbAPIClient()

    def fetch_wikipedia_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a Wikipedia page"""
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Wikipedia page: {e}")
            return None

    def extract_movies_from_tables(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract movies from Wikipedia tables"""
        tables = [
            table for table in soup.find_all('table', class_='wikitable')
            if not table.find('caption')
        ]

        wiki_movies = []
        for table in tables:
            rows = table.find_all('tr')[1:]
            prev_month = None
            prev_date = None

            for row in rows:
                cells = row.find_all(['td', 'th'])
                movie = self._parse_table_row(cells, prev_month, prev_date)

                if movie:
                    # Update previous values for next iteration
                    if len(cells) == 6:
                        prev_month = cells[0].get_text(strip=True)
                        prev_date = cells[1].get_text(strip=True)
                    elif len(cells) == 5:
                        prev_date = cells[0].get_text(strip=True)

                    wiki_movies.append(movie)

        return wiki_movies

    def _parse_table_row(self, cells: List, prev_month: str, prev_date: str) -> Optional[Dict]:
        """Parse a single table row to extract movie data"""
        movie = {}

        if len(cells) == 6:
            prev_month = cells[0].get_text(strip=True)
            prev_date = cells[1].get_text(strip=True)
            movie = {
                'Release Date': f"{prev_date}, {prev_month}",
                'Title': cells[2].get_text(strip=True),
                'Studio': cells[3].get_text(strip=True),
                'Cast and Crew': cells[4].get_text(strip=True)
            }
        elif len(cells) == 5:
            prev_date = cells[0].get_text(strip=True)
            movie = {
                'Release Date': f"{prev_date}, {prev_month}",
                'Title': cells[1].get_text(strip=True),
                'Studio': cells[2].get_text(strip=True),
                'Cast and Crew': cells[3].get_text(strip=True)
            }
        elif len(cells) == 4:
            movie = {
                'Release Date': f"{prev_date}, {prev_month}",
                'Title': cells[0].get_text(strip=True),
                'Studio': cells[1].get_text(strip=True),
                'Cast and Crew': cells[2].get_text(strip=True)
            }

        return movie if movie else None

    def enrich_movie_with_tmdb(self, movie: Dict[str, str]) -> Dict:
        """Enrich Wikipedia movie data with TMDb information"""
        logger.info(f"Fetching TMDb data for movie: {movie['Title']}")
        tmdb_data = self.api_client.search_movie_by_title(movie['Title'])

        if tmdb_data:
            production_companies = extract_names(tmdb_data.get('production_companies', []), 'name')
            genres = extract_names(tmdb_data.get('genres', []), 'name')

            # Fetch credits
            movie_id = tmdb_data.get('id')
            credits = self.api_client.get_movie_credits(movie_id) if movie_id else {"directors": [], "actors": []}
            directors = ', '.join(credits.get('directors', []))
            actors = format_actors(credits.get('actors', []))

            return {
                'tmdb_id': tmdb_data.get('id'),
                'title': movie.get('Title'),
                'budget': tmdb_data.get('budget'),
                'revenue': tmdb_data.get('revenue'),
                'rating': tmdb_data.get('vote_average'),
                'vote_count': tmdb_data.get('vote_count'),
                'release_date': tmdb_data.get('release_date') or movie.get('Release Date'),
                'original_language': tmdb_data.get('original_language'),
                'production_companies': production_companies,
                'genres': genres,
                'directors': directors,
                'actors': actors,
                'runtime': tmdb_data.get('runtime')
            }
        else:
            # Return basic structure with empty TMDb fields
            return {
                'tmdb_id': None,
                'title': movie.get('Title'),
                'budget': None,
                'revenue': None,
                'rating': None,
                'vote_count': None,
                'release_date': movie.get('Release Date'),
                'original_language': None,
                'production_companies': "",
                'genres': "",
                'directors': "",
                'actors': "",
                'runtime': None
            }

    def process_movies(self, wiki_movies: List[Dict[str, str]]) -> List[Dict]:
        """Process all movies with TMDb enrichment using parallel execution"""
        all_movies = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.enrich_movie_with_tmdb, movie) for movie in wiki_movies]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    all_movies.append(result)
                except Exception as exc:
                    logger.error(f"Exception occurred during TMDb fetch: {exc}")
                time.sleep(0.25)  # Be polite to TMDb API

        return all_movies

def main():
    """Main function"""
    scraper = WikipediaMovieScraper()

    logger.info("Fetching Wikipedia page...")
    soup = scraper.fetch_wikipedia_page(WIKI_URL)
    if not soup:
        logger.error("No soup object returned, exiting.")
        return

    logger.info("Extracting movies from tables...")
    wiki_movies = scraper.extract_movies_from_tables(soup)

    logger.info("Enriching movies with TMDb data...")
    movies = scraper.process_movies(wiki_movies)

    logger.info(f"Extracted {len(movies)} movies.")
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    save_movies_to_csv(movies, output_path)
    logger.info(f"Saved movies to {output_path}")

if __name__ == "__main__":
    main()
