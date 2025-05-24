def extract_names(items: list, key: str) -> str:
    return ', '.join(item.get(key, '') for item in items if item.get(key))

def format_actors(actors: list) -> str:
    return '; '.join([f"{a['name']} ({a.get('character', '')})" for a in actors if a.get('name')])
