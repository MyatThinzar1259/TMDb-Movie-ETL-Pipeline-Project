import requests
import pandas as pd
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure the logging module
logging.basicConfig(
    level=logging.DEBUG,  # Set the minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    filename="movie.log"
)

# Get the API key from the environment
api_key = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"


# Function to fetch data from URL
def extract_json_from_url(url:str , params:dict ) -> dict:
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except Exception as e:
        logging.error(f"Failed to extract data from {url}: {e}")
        raise ConnectionError(f"Can't extract data from {url} : {e}")

# def get_popular_movies(pages: int = 1) -> list[dict]:
    
#     movies_list = []
    
#     for page in range(1, pages + 1): #loop for multipage  
#         logging.info(f"Fetching page {page} of popular movies...")
#         url = f"{BASE_URL}/movie/popular"
#         params = {"api_key": api_key, "language": "en-US", "page": page}
#         movies_json = extract_json_from_url(url , params)
#         for movie in movies_json.get("results", []):
#             data_dict ={
#             'id': movie['id'],
#             'title': movie['title'],
#             'overview': movie['overview'],
#             'rating': movie['vote_average'],
#             'release_date': movie['release_date'],
#             'genres': movie['genre_ids'] # Genre IDs will be mapped later
#                 }
#             movies_list.append(data_dict)    

#     return movies_list

# Function to get movie genres
def get_genres() -> list[dict]:
    url = f"{BASE_URL}/genre/movie/list"
    params ={"api_key": api_key, "language" : "en-US" }
    genres_json = extract_json_from_url(url , params)
    return genres_json.get("genres", [])

# Function to get movie details (for genres and other metadata)
def get_movie_details(movie_id) -> dict:
    url = f"{BASE_URL}/movie/{movie_id}"
    params ={"api_key": api_key, "language" : "en-US" }
    return extract_json_from_url(url , params)

# def extract_data(pages = 1) -> tuple[list[dict], list[dict], dict[int, dict], dict[str, dict]]:
#     #movies = get_popular_movies(pages)
#     genres = get_genres()
#     companies = {}
#     languages  = {}

#     for movie in results:
#         movie_details = get_movie_details(movie['id'])

#         #to get the data of production companies for each movie
#         for company in movie_details.get("production_companies", []):
#             companies[company['id']] = {
#                 'company_id' : company['id'],
#                 'company_name' : company['name']
#             }
        
#         # to get  language data of each movie
#         lang_code = movie_details.get("original_language", "unknown")
#         if lang_code not in languages:
#             languages[lang_code] = {
#                 "language_code": lang_code,
#                 "language_name": lang_code  # Could map "en" → "English" later
#             }
        
#         # add companies and genres field to movie data dict
#         movie["production_companies"] = movie_details.get("production_companies", [])
#         movie["genres_detail"] = movie_details.get("genres", [])

#     return movies, genres, companies, languages




# if __name__ == "__main__":
#     data = extract_data()
#     print(data)
    





    

    

