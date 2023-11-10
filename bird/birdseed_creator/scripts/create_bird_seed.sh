#!/bin/bash

# create_bird_seed
rm -rf ../results/regdna/*
rm -rf ../results/xmi/*
cd ../src/
pip install pyecore
pip install unidecode
export PYTHONPATH=../src:../../regna
python create_model_from_website.py
