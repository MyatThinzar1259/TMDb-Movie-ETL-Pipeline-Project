import pandas as pd
import os
from typing import Optional
from datetime import datetime
from utils_transformer import (
    load_csv_to_dataframe,
    save_dataframe_to_csv,
    clean_text,
    parse_date
)

def transform_wiki_data(input_path: str, output_dir: str = "clean_data") -> Optional[pd.DataFrame]:
    """Transform Wikipedia data from raw to clean format."""
    # Load raw data
    df = load_csv_to_dataframe(input_path)
    if df is None or df.empty:
        return None
    
    # Apply transformations
    df['Title'] = df['Title'].apply(clean_text)
    df['Studio'] = df['Studio'].apply(clean_text)
    df['Cast and Crew'] = df['Cast and Crew'].apply(clean_text)
    
    # Parse and standardize release date
    def parse_wiki_date(date_str):
        if pd.isna(date_str):
            return None
        try:
            # Example format: "15, January"
            date_part, month_part = date_str.split(', ')
            date_obj = datetime.strptime(f"{date_part} {month_part} 2024", "%d %B %Y")
            return date_obj.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            return None
    
    df['ReleaseDate'] = df['Release Date'].apply(parse_wiki_date)
    
    # Select and rename columns
    df = df[['Title', 'Studio', 'Cast and Crew', 'ReleaseDate']]
    df = df.rename(columns={
        'Cast and Crew': 'CastCrew'
    })
    
    # Add source column
    df['Source'] = 'Wikipedia'
    
    # Save transformed data
    output_filename = "clean_wiki_american_movies.csv"
    output_path = os.path.join(output_dir, output_filename)
    save_dataframe_to_csv(df, output_path)
    
    return df

if __name__ == "__main__":
    input_path = os.path.join("raw_data", "american_movies_2024.csv")
    print("\n[INFO] Processing Wikipedia file")
    transform_wiki_data(input_path)