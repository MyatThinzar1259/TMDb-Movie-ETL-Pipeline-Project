import requests
import os
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any

load_dotenv()

api_key = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

def extract_json_from_url(url: str, params: dict) -> dict:
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.debug(f"Successfully fetched data from {url}")
        return data
    except requests.exceptions.Timeout:
        logging.error(f"Timeout occurred while fetching data from {url}")
        print(f"[ERROR] Timeout occurred while fetching data from {url}")
        raise
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while fetching data from {url}: {http_err}")
        print(f"[ERROR] HTTP error occurred while fetching data from {url}: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request exception occurred while fetching data from {url}: {req_err}")
        print(f"[ERROR] Request exception occurred while fetching data from {url}: {req_err}")
        raise
    except ValueError as json_err:
        logging.error(f"JSON decoding failed for {url}: {json_err}")
        print(f"[ERROR] JSON decoding failed for {url}: {json_err}")
        raise

def get_genres() -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/genre/movie/list"
    params = {"api_key": api_key, "language": "en-US"}
    genres_json = extract_json_from_url(url, params)
    return genres_json.get("genres", [])

def get_movie_details(movie_id: int) -> Dict[str, Any]:
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {"api_key": api_key, "language": "en-US"}
    return extract_json_from_url(url, params)









