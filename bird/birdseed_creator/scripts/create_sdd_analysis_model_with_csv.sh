#!/bin/bash

# clear results script

rm -rf ecore4reg/python/results/xmi/*
rm -rf ecore4reg/python/results/csv/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python create_sdd_analysis_model_with_csv.py
