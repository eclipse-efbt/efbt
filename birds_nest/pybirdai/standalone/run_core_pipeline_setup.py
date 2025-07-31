import os

os.system("uv run pybirdai/standalone/standalone_fetch_artifacts_eldm.py")
os.system("uv run pybirdai/standalone/standalone_setup_migrate_database.py")
