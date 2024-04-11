#!/bin/bash

# clear results script
export PYTHONPATH="/workspaces/efbt/bird/birdseed_creator/src:/workspaces/efbt/bird/regdna_python_model:/workspaces/efbt/bird/sdd_python_model"
rm -rf bird/birdseed_creator/results/generations_transformations_csv/*
rm -rf bird/birdseed_creator/results/regdna/*
rm -rf bird/birdseed_creator/results/csv/*
rm -rf bird/birdseed_creator/results/xmi/*

cd bird/birdseed_creator/src/entry_points
pip install pyecore
pip install unidecode
python create_outline_transformations_ae.py
