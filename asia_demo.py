import requests
import logging
import pandas as pd
import time
import os
import csv
import glob
from dotenv import load_dotenv
from extract import get_genres, get_movie_details, extract_json_from_url  # Import functions from extract.py


# Load environment variables from .env file
load_dotenv()

# Configure the logging module
logging.basicConfig(
    level=logging.DEBUG,  # Set the minimum logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log message format
    filename="asia_demo.log"
)

# Get the API key from the environment
api_key = os.getenv("API_KEY")
BASE_URL = "https://api.themoviedb.org/3"

OUTPUT_FOLDER = "tmdb_movies_filtered"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def save_movies_to_csv(movies: list, filename: str):
    with open(filename, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'id', 'title', 'overview', 'rating', 'release_date',
            'original_language', 'genres' , 'production_companies'
        ])
        for movie in movies:
            writer.writerow([
                movie['id'],
                movie['title'],
                movie['overview'],
                movie['rating'],
                movie['release_date'],
                movie['original_language'],
                movie['production_companies'],
                movie['genres'],
            ])

def get_movies_by_language(language_code: str, max_pages: int = 500, save_every: int = 50):
    all_movies = []
    genres = get_genres()
    genre_map = {g['id']: g['name'] for g in genres}
    companies = {}
    languages  = {}

    start_page = 1
    # Loop until reaching the max page limit
    while start_page <= max_pages:
        
        # Define the last page of this batch
        end_page = min(start_page + save_every - 1, max_pages)
        # Define the CSV filename
        filename = f"{OUTPUT_FOLDER}/{language_code}_movies_p{start_page}_to_p{end_page}.csv"

        # Skip fetching if this file already exists
        if os.path.exists(filename):
            logging.info(f" Skipping {language_code} pages {start_page}-{end_page}, file exists.")
            start_page += save_every
            continue

        logging.info(f" Fetching {language_code} pages {start_page} to {end_page}...")
        movies_list = []
        for page in range(start_page, end_page + 1):
            url = f"{BASE_URL}/discover/movie"
            params = {
            'api_key': api_key,
            'language': 'en-US',
            'page': page,
            'with_original_language': language_code,
            'primary_release_date.gte': '2024-01-01',
            'primary_release_date.lte': '2024-12-31'
        }

            try:

                movie_json = extract_json_from_url(url, params)
                results = movie_json.get('results', [])
                print(f"Page {page}: {len(results)} results")

                if not results:
                   break

                for movie in results:
                    movie_details = get_movie_details(movie['id'])  # Call TMDb API again for details

                    #to get the data of production companies for each movie
                    for company in movie_details.get("production_companies", []):
                        companies[company['id']] = {
                            'company_id' : company['id'],
                            'company_name' : company['name']
                        }

                        # to get  language data of each movie
                    lang_code = movie_details.get("original_language", "unknown")
                    if lang_code not in languages:
                        languages[lang_code] = {
                            "language_code": lang_code,
                            "language_name": lang_code  # Could map "en" → "English" later
                                }
                            
                    movie_data = {
                        'id': movie['id'],
                        'title': movie['title'],
                        'overview': movie['overview'],
                        'rating': movie['vote_average'],
                        'release_date': movie['release_date'],
                        'original_language': movie['original_language'],
                        'production_companies': ','.join([c['name'] for c in movie_details.get("production_companies", [])]),
                        'genres': ','.join([genre_map.get(gid, str(gid)) for gid in movie['genre_ids']])
                        }
                    movies_list.append(movie_data)
                    all_movies.append(movie_data)
                time.sleep(0.25)
            except Exception as e:
                    logging.error(f" Failed on page {page}: {e}")
                    continue
        
        if movies_list:
        # Save the batch of movies to CSV
            save_movies_to_csv(movies_list, filename)
            logging.info(f" Saved {len(movies_list)} movies to {filename}")
            print(f"Saved {len(movies_list)} movies to {filename}")
        else:
            logging.warning(f"No movies collected in batch {start_page}-{end_page}")
        # Clear buffer and move to the next batch
        start_page += save_every

    logging.info(f" Done fetching for language {language_code}")
    data_dict = {"movies_list": all_movies ,  
                 "genres" : genres,
                 "companies" : companies,
                 "languages" : languages}
    return data_dict

def merge_csvs_by_language(language_code: str, input_folder: str = OUTPUT_FOLDER, output_folder: str = "tmdb_merged_csv"):
    os.makedirs(output_folder, exist_ok=True)

    pattern = os.path.join(input_folder, f"{language_code}_movies_p*_to_p*.csv")
    csv_files = sorted(glob.glob(pattern))

    if not csv_files:
        logging.warning(f"No partial CSVs found for {language_code} to merge.")
        return

    # Read and concatenate all partial CSVs
    df_list = [pd.read_csv(file) for file in csv_files]
    merged_df = pd.concat(df_list, ignore_index=True)

    # Save the merged file
    merged_filename = os.path.join(output_folder, f"{language_code}_movies_all.csv")
    merged_df.to_csv(merged_filename, index=False)
    logging.info(f"Merged {len(csv_files)} CSVs into {merged_filename} ({len(merged_df)} rows)")

    return len(merged_df)



if __name__ == "__main__":
    # Run for  selected languages
    languages = ['ko', 'ja', 'th', 'tl']  # Korean, Japanese, Thai, Filipino
    for lang in languages:
        get_movies_by_language(lang)
        total_rows = merge_csvs_by_language(lang)
        print(f" {lang} → {total_rows} movies")

# tota movies 
# ko = 811
# ja = 1009
# th = 293
# tl = 507