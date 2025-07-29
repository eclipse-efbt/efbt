import os

if "bird_data_model.py" in os.listdir(f"pybirdai{os.sep}models"):
    from pybirdai.models.bird_data_model import *
