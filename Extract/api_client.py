import os
import time
import logging
import requests
from typing import Optional, Dict, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TMDbAPIClient:
    """Common TMDb API client for shared functionality"""

    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.base_url = "https://api.themoviedb.org/3"
        self.max_retries = 3
        self.logger = logging.getLogger(__name__)

    def make_request_with_retries(
        self,
        url: str,
        params: Dict,
        timeout: int = 20,
        retry_delay: int = 3
    ) -> Optional[Dict]:
        """Make HTTP request with retry logic"""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Fetching URL: {url} (Attempt {attempt}/{self.max_retries})")
                response = requests.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed on attempt {attempt}: {e}")
                if attempt < self.max_retries:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"Max retries reached for URL: {url}")
                    return None

    def get_movie_details(self, movie_id: int) -> Dict:
        """Fetch basic movie details"""
        url = f"{self.base_url}/movie/{movie_id}"
        params = {
            "api_key": self.api_key,
            "language": "en-US"
        }
        data = self.make_request_with_retries(url, params)
        return data if data is not None else {}

    def get_movie_credits(self, movie_id: int) -> Dict:
        """Fetch credits (directors and actors) for a movie"""
        url = f"{self.base_url}/movie/{movie_id}/credits"
        params = {
            "api_key": self.api_key,
            "language": "en-US"
        }
        data = self.make_request_with_retries(url, params, retry_delay=3)

        if data is None:
            return {"directors": [], "actors": []}

        # Extract directors
        directors = [
            person["name"]
            for person in data.get("crew", [])
            if person.get("job") == "Director"
        ]
        
        # Extract top 5 main actors with their characters
        actors = [
            {
                "name": person["name"],
                "character": person["character"]
            }
            for person in data.get("cast", [])[:5]
        ]   

        return {
            "directors": directors,
            "actors": actors
        }

    def get_movie_full_details(self, movie_id: int) -> Dict:
        """Combine basic details and credits into one response"""
        details = self.get_movie_details(movie_id)
        credits = self.get_movie_credits(movie_id)

        return {
            **details,
            "directors": credits["directors"],
            "actors": credits["actors"]
        }

    def search_movie_by_title(self, title: str) -> Optional[Dict]:
        """Search TMDb for a movie by title and return detailed info"""
        search_url = f"{self.base_url}/search/movie"
        params = {
            "api_key": self.api_key,
            "query": title,
            "language": "en-US",
            "page": 1,
            "include_adult": False
        }

        try:
            self.logger.info(f"Searching TMDb for: {title}")
            response = requests.get(search_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("results"):
                movie_id = data["results"][0].get("id")
                if movie_id:
                    self.logger.info(f"Fetching TMDb details for movie ID: {movie_id} ({title})")
                    return self.get_movie_full_details(movie_id)
            return None
        except Exception as e:
            self.logger.error(f"TMDb API error for '{title}': {e}")
            return None
