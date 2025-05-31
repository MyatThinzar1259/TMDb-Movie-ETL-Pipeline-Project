import os
import sys
import pytest

# Add project root and subfolders to sys.path for module resolution in tests
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "Extract"))
sys.path.insert(0, os.path.join(project_root, "Transform"))
sys.path.insert(0, os.path.join(project_root, "Load"))

def test_db_connection():
    print("\n[TEST] test_db_connection: started")
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    load_dotenv()
    import os
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;"))
        assert result.scalar() == 1
    print("[TEST] test_db_connection: passed")

# def test_create_tables():
#     print("\n[TEST] test_create_tables: started")
#     from Load.create_table_in_postgres import create_tables
#     # Should not raise
#     create_tables()
#     print("[TEST] test_create_tables: passed")

# def test_insert_data(monkeypatch):
#     print("\n[TEST] test_insert_data: started")
#     from Load.insert_data_to_postgres import main as insert_main
#     # Monkeypatch input/output if needed, here just check it runs
#     try:
#         insert_main()
#         print("[TEST] test_insert_data: passed")
#     except Exception as e:
#         print(f"[TEST] test_insert_data: skipped ({e})")
#         pytest.skip(f"Insert data step failed (likely missing test DB or data): {e}")

# def test_tmdb_transformer_runs(tmp_path):
#     print("\n[TEST] test_tmdb_transformer_runs: started")
#     from Transform.tmdb_transformer import process_all_tmdb_files
#     # Create empty input dir to ensure function runs
#     input_dir = tmp_path / "tmdb"
#     input_dir.mkdir()
#     process_all_tmdb_files(str(input_dir), str(tmp_path))
#     print("[TEST] test_tmdb_transformer_runs: passed")

# def test_wiki_transformer_runs(tmp_path):
#     print("\n[TEST] test_wiki_transformer_runs: started")
#     from Transform.wiki_transformer import process_all_wiki_files
#     input_dir = tmp_path / "wiki"
#     input_dir.mkdir()
#     process_all_wiki_files(str(input_dir), str(tmp_path))
#     print("[TEST] test_wiki_transformer_runs: passed")

# def test_normalize_csv_to_json_runs(tmp_path):
#     print("\n[TEST] test_normalize_csv_to_json_runs: started")
#     from Transform.normalize_csv_to_json import DataNormalizer
#     # Create empty input dir to ensure function runs
#     input_dir = tmp_path / "clean_data"
#     input_dir.mkdir()
#     normalizer = DataNormalizer(csv_dir=str(input_dir), output_dir=str(tmp_path))
#     normalizer.process_files()
#     normalizer.export_to_json()
#     print("[TEST] test_normalize_csv_to_json_runs: passed")

# def test_main_py_runs(monkeypatch):
#     print("\n[TEST] test_main_py_runs: started")
#     import main
#     # Monkeypatch sys.exit to prevent exit on error
#     monkeypatch.setattr("sys.exit", lambda code=0: (_ for _ in ()).throw(SystemExit(code)))
#     # Call main.main() or main.<function> if available, else just import to check for errors
#     if hasattr(main, "main"):
#         try:
#             main.main()
#             print("[TEST] test_main_py_runs: passed")
#         except SystemExit:
#             pass
#     else:
#         print("[TEST] test_main_py_runs: main() not found, importing main.py only.")
#         print("[TEST] test_main_py_runs: passed")

# To run these tests, use the following command from your project root:
#    pytest tests/test_etl_pipeline.py
#
# Or to run all tests in the tests/ directory:
#    pytest tests
#
# You will see print statements for each test in the output.
