#!/bin/bash

# clear results script
export PYTHONPATH="/workspaces/efbt/bird/birdseed_creator/src:/workspaces/efbt/bird/regdna_python_model:/workspaces/efbt/bird/sdd_python_model"


cd bird/birdseed_creator/src/entry_points
pip install pyecore
pip install unidecode
python get_unreachable_members.py
