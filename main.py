import sys
import os

# Add project root and subfolders to sys.path for module resolution
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "Extract"))
sys.path.insert(0, os.path.join(project_root, "Transform"))
sys.path.insert(0, os.path.join(project_root, "Load"))

# --- Extract Step ---
print("\n[ETL] Step 1: Extracting TMDb data...")
try:
    from Extract.tmdb import main as tmdb_extract_main
    tmdb_extract_main()
except Exception as e:
    print(f"[ERROR] TMDb extraction failed: {e}")
    sys.exit(1)

print("\n[ETL] Step 2: Extracting Wikipedia data...")
try:
    from Extract.wiki import main as wiki_extract_main
    wiki_extract_main()
except Exception as e:
    print(f"[ERROR] Wikipedia extraction failed: {e}")
    sys.exit(1)

# --- Transform Step ---
print("\n[ETL] Step 3: Transforming TMDb data...")
try:
    from Transform.tmdb_transformer import process_all_tmdb_files
    process_all_tmdb_files()
except Exception as e:
    print(f"[ERROR] TMDb transformation failed: {e}")
    sys.exit(1)

print("\n[ETL] Step 4: Transforming Wikipedia data...")
try:
    from Transform.wiki_transformer import process_all_wiki_files
    process_all_wiki_files()
except Exception as e:
    print(f"[ERROR] Wikipedia transformation failed: {e}")
    sys.exit(1)

# --- Normalize Step ---
print("\n[ETL] Step 5: Normalizing and exporting to JSON...")
try:
    from Transform.normalize_csv_to_json import main as normalize_main
    normalize_main()
except Exception as e:
    print(f"[ERROR] Normalization failed: {e}")
    sys.exit(1)

# --- Load Step ---
print("\n[ETL] Step 6: Creating tables in PostgreSQL...")
try:
    from Load.create_table_in_postgres import create_tables
    create_tables()
except Exception as e:
    print(f"[ERROR] Table creation failed: {e}")
    sys.exit(1)

print("\n[ETL] Step 7: Inserting data into PostgreSQL...")
try:
    from Load.insert_data_to_postgres import main as load_main
    load_main()
except Exception as e:
    print(f"[ERROR] Data loading failed: {e}")
    sys.exit(1)

print("\n[ETL] Pipeline completed successfully!")
