import requests
from bs4 import BeautifulSoup
import os
from typing import List, Dict, Optional
import logging
from utils_csv import save_movies_to_csv

WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_American_films_of_2024'
OUTPUT_DIR = "raw_data"
OUTPUT_FILE = "american_movies_2024.csv"

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/hw_movie_fetch.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_wikipedia_page(url: str) -> Optional[BeautifulSoup]:
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        logging.error(f"Failed to fetch Wikipedia page: {e}") 
        return None

def extract_movies_from_tables(soup: BeautifulSoup) -> List[Dict[str, str]]:
    tables = []
    for table in soup.find_all('table', class_='wikitable'):
        if not table.find('caption'):
            tables.append(table)
            
    all_movies = []
    for table in tables:
        rows = table.find_all('tr')[1:]
        prev_month = None
        prev_date = None
        for row in rows:
            cells = row.find_all(['td', 'th'])
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

            if(movie):
                all_movies.append(movie)
    return all_movies

def main():
    logging.info(" Fetching Wikipedia page...")
    soup = fetch_wikipedia_page(WIKI_URL)
    if not soup:
        return
    logging.info(" Extracting movies from tables...")
    movies = extract_movies_from_tables(soup)
    logging.info(f" Extracted {len(movies)} movies.")
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    save_movies_to_csv(movies, output_path)

if __name__ == "__main__":
    main()
