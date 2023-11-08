#!/bin/bash

# create_bird_seed
rm -rf ecore4reg/python/results/regdna/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python create_model_from_website.py
