#!/bin/bash

# clear results script

rm -rf ecore4reg/python/results_website/ecore/*
rm -rf ecore4reg/python/results_website/ecore4reg/*
rm -rf ecore4reg/python/results_website/json/*
rm -rf ecore4reg/python/results_website/matches/*
rm -rf ecore4reg/python/results_website/xcore/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python generate_generation_rules.py
