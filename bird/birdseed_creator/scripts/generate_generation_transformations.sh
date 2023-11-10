#!/bin/bash

# clear results script


rm -rf regdna/results/regdna/*
rm -rf regdna/results/generations_transformations_csv/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python generate_generation_rules.py
