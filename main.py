import os
import sys
import logging

# --- Ensure logs directory exists before configuring logging ---
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, "etl_pipeline.log"), mode='w')
    ]
)
logger = logging.getLogger(__name__)

# --- Add project root and subfolders to sys.path ---
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "Extract"))
sys.path.insert(0, os.path.join(project_root, "Transform"))
sys.path.insert(0, os.path.join(project_root, "Load"))

def run_step(step_name: str, func):
    logger.info(f"[ETL] Step: {step_name}")
    try:
        func()
        logger.info(f"[ETL] {step_name} completed successfully.")
    except Exception as e:
        logger.exception(f"[ERROR] {step_name} failed: {e}")
        sys.exit(1)

def step_1_extract_tmdb():
    from Extract.tmdb import main as tmdb_extract_main
    tmdb_extract_main()

def step_2_extract_wiki():
    from Extract.wiki import main as wiki_extract_main
    wiki_extract_main()

def step_3_transform_tmdb():
    from Transform.tmdb_transformer import process_all_tmdb_files
    process_all_tmdb_files()

def step_4_transform_wiki():
    from Transform.wiki_transformer import process_all_wiki_files
    process_all_wiki_files()

def step_5_normalize_json():
    from Load.data_normalizer import main as normalize_main
    normalize_main()

def step_6_start_fact_builder():
    from Load.star_fact_builder import main as start_fact_builder_main
    start_fact_builder_main()

def step_7_create_tables():
    from Load.create_table_in_postgres import create_tables
    create_tables()

def step_8_insert_data():
    from Load.load_json_to_postgres import main as load_main
    load_main()

# --- Run ETL Pipeline ---
if __name__ == "__main__":
    logger.info("[ETL] Starting full ETL pipeline...")

    run_step("Step 1: Extracting TMDb data", step_1_extract_tmdb)
    run_step("Step 2: Extracting Wikipedia data", step_2_extract_wiki)
    run_step("Step 3: Transforming TMDb data", step_3_transform_tmdb)
    run_step("Step 4: Transforming Wikipedia data", step_4_transform_wiki)
    run_step("Step 5: Normalizing and exporting to JSON", step_5_normalize_json)
    run_step("Step 6: Starting star fact builder", step_6_start_fact_builder)
    run_step("Step 7: Creating PostgreSQL tables", step_7_create_tables)
    run_step("Step 8: Loading data into PostgreSQL", step_8_insert_data)

    logger.info("[ETL] Pipeline completed successfully")
