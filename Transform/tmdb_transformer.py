import pandas as pd
import os
from typing import Optional
from utils_transform import (
    load_csv_to_dataframe,
    save_dataframe_to_csv,
    clean_text,
    parse_date,
    standardize_language_code
)

def transform_tmdb_data(input_path: str, output_dir: str = "clean_data") -> Optional[pd.DataFrame]:
    """Transform TMDB data from raw to clean format."""
    # Extract language code from filename
    lang_code = os.path.basename(input_path).split('_')[0]
    
    # Load raw data
    df = load_csv_to_dataframe(input_path)
    if df is None or df.empty:
        return None
    
    # Apply transformations
    df['title'] = df['title'].apply(clean_text)
    df['original_language'] = df['original_language'].apply(standardize_language_code)
    df['release_date'] = df['release_date'].apply(lambda x: parse_date(x) if pd.notna(x) else None)
    df['production_companies'] = df['production_companies'].apply(clean_text)
    df['genres'] = df['genres'].apply(clean_text)
    df['source'] = 'TMDB'
    
    # Save transformed data
    output_filename = f"clean_tmdb_{lang_code}_movies_2024.csv"
    output_path = os.path.join(output_dir, output_filename)
    save_dataframe_to_csv(df, output_path)
    
    return df

def process_all_tmdb_files(input_dir: str = "raw_data", output_dir: str = "clean_data"):
    """Process all TMDB CSV files in the input directory."""
    if not os.path.exists(input_dir):
        print(f"[ERROR] Input directory {input_dir} does not exist")
        return
    
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv") and any(lang in filename for lang in ['hi', 'ko', 'jp', 'th', 'tl']):
            input_path = os.path.join(input_dir, filename)
            print(f"\n[INFO] Processing TMDB file: {filename}")
            transform_tmdb_data(input_path, output_dir)

if __name__ == "__main__":
    process_all_tmdb_files()