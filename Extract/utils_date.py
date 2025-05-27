from datetime import datetime

def convert_movie_date(date_str, default_year=2024):
    """
    Convert date from "12, JULY" format to "YYYY-MM-DD" format
    
    Args:
        date_str (str): Date string in format "DD, MONTH"
        default_year (int): Year to use when not specified in input
    
    Returns:
        str: Date in YYYY-MM-DD format or None if invalid
    """
    if not date_str:
        return None
    
    try:
        # Clean the input string
        date_str = date_str.strip()
        
        # Split by comma and space
        parts = date_str.split(', ')
        if len(parts) != 2:
            return None
            
        day, month = parts
        
        # Parse using datetime
        # Add the default year to make it parseable
        full_date_str = f"{day} {month} {default_year}"
        parsed_date = datetime.strptime(full_date_str, "%d %B %Y")
        
        # Format to YYYY-MM-DD
        return parsed_date.strftime("%Y-%m-%d")
        
    except (ValueError, AttributeError) as e:
        print(f"Error parsing date '{date_str}': {e}")
        return None