#!/bin/bash

# clear results script

rm -rf ecore4reg/python/results_website/ecore/*
rm -rf ecore4reg/python/results_website/ecore4reg/*
rm -rf ecore4reg/python/results_website/json/*
rm -rf ecore4reg/python/results_website/matches/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python run_matching_algorithm_from_website.py


