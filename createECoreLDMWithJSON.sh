#!/bin/bash

# clear results script

rm -rf ecore4reg/python/results/ecore/*
rm -rf ecore4reg/python/results/ecore4reg/*
rm -rf ecore4reg/python/results/json/*
rm -rf ecore4reg/python/results/matches/*
rm -rf ecore4reg/python/results/xcore/*
rm -rf ecore4reg/python/results/sdd/*
rm -rf ecore4reg/python/results/xmi/*
rm -rf ecore4reg/python/results/generations_transformations_csv/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python create_ecore_ldm_with_json.py


