import pandas as pd
import os
from typing import Optional
from datetime import datetime
from utils_transformer import (
    load_csv_to_dataframe,
    save_dataframe_to_csv,
    clean_text,
    parse_date,
    standardize_language_code
)

def transform_wiki_data(input_path: str, output_dir: str = "Data/clean_data") -> Optional[pd.DataFrame]:
    """Transform Wikipedia data from raw to clean format."""
    try:
        # Load raw data
        df = load_csv_to_dataframe(input_path)
        if df is None or df.empty:
            print("[ERROR] No data loaded or empty DataFrame")
            return None

        # Remove rows where 'tmdb_id' is missing
        df = df.dropna(subset=['tmdb_id'])

        # Clean text fields
        text_columns = ['title','production_companies', 'genres', 'directors', 'actors']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(clean_text)


        df['rating'] = df['rating'].round(1)
        df['vote_count'] = pd.to_numeric(df['vote_count'], errors='coerce')
        df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce')
        df['release_date'] = df['release_date'].apply(parse_date)
        df['original_language'] = df['original_language'].apply(standardize_language_code)

        output_columns = [
            'tmdb_id', 'title', 'budget', 'revenue',
            'rating', 'release_date', 'original_language', 'production_companies',
            'genres', 'directors', 'actors', 'vote_count', 'runtime'
        ]

        # Only keep columns that exist in the dataframe
        df = df[[col for col in output_columns if col in df.columns]]

        # Add source column
        df['source'] = 'Wikipedia'

        # Save transformed data
        output_filename = "clean_wiki_tmdb_en_movies_2024.csv"
        output_path = os.path.join(output_dir, output_filename)

        # Try alternative save method if permission denied
        try:
            save_dataframe_to_csv(df, output_path)
        except PermissionError:
            # Try saving to current directory if target directory fails
            alt_path = os.path.join(os.getcwd(), output_filename)
            print(f"[WARNING] Could not save to {output_path}, trying {alt_path}")
            save_dataframe_to_csv(df, alt_path)
            return df

        return df

    except Exception as e:
        print(f"[ERROR] Transformation failed: {str(e)}")
        return None

if __name__ == "__main__":
    input_path = os.path.join("Data", "raw_data", "en_movies_2024.csv")
    print("\n[INFO] Processing Wikipedia/TMDb combined file")
    result = transform_wiki_data(input_path)
    if result is not None:
        print("[SUCCESS] Data transformation completed")
    else:
        print("[ERROR] Data transformation failed")
