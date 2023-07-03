#!/bin/bash

# clear results script

rm -rf ecore4reg/python/resultsfinrep28/ecore/*
rm -rf ecore4reg/python/resultsfinrep28/ecore4reg/*
rm -rf ecore4reg/python/resultsfinrep28/json/*
rm -rf ecore4reg/python/resultsfinrep28/xmi/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python create_transformations_from_vtl_with_json.py


