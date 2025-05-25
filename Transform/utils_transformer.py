import pandas as pd
import os
from typing import List, Dict, Optional
import re
from datetime import datetime

def load_csv_to_dataframe(file_path: str) -> Optional[pd.DataFrame]:
    """Load CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(file_path)
        print(f"[SUCCESS] Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load CSV file {file_path}: {e}")
        return None

def save_dataframe_to_csv(
    df: pd.DataFrame,
    output_path: str,
    index: bool = False
) -> bool:
    """Save DataFrame to CSV file."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=index)
        print(f"[SUCCESS] Saved {len(df)} records to {output_path}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to save DataFrame to CSV: {e}")
        return False

def clean_text(text: str) -> str:
    """Clean text by removing special characters and extra whitespace."""
    if pd.isna(text):
        return ""
    # Remove special characters except alphanumeric, spaces, and basic punctuation
    text = re.sub(r'[^\w\s.,-]', '', text)
    # Collapse multiple spaces into one
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def parse_date(date_str: str, format: str = "%Y-%m-%d") -> Optional[str]:
    """Parse date string into standardized format."""
    if pd.isna(date_str):
        return None
    try:
        date_obj = datetime.strptime(date_str, format)
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return None

def standardize_language_code(code: str) -> str:
    """Standardize language codes to consistent format."""
    code_map = {
        'hi': 'Hindi',
        'ko': 'Korean',
        'jp': 'Japanese',
        'th': 'Thai',
        'tl': 'Filipino',
        'en': 'English'
    }
    return code_map.get(code.lower(), code.upper())