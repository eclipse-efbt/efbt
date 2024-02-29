#!/bin/bash

# clear results script
export PYTHONPATH="/workspaces/efbt/bird/bird_utilities/ldm_tools/src:/workspaces/efbt/bird/regdna_python_model"
rm -rf /workspaces/efbt/bird/bird_utilities/ldm_tools/results/csv/*
rm -rf /workspaces/efbt/bird/bird_utilities/ldm_tools/results/regdna/*

cd /workspaces/efbt/bird/bird_utilities/ldm_tools/src/ldm_entry_points
pip install pyecore
pip install unidecode
python create_ldm_and_explode_subtypes.py