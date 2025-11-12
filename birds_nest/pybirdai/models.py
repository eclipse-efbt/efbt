import os

if "lineage_model.py" in os.listdir(f"pybirdai{os.sep}models"):
    from pybirdai.models.lineage_model import *
