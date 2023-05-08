#!/bin/bash

# clear results script

rm -rf ecore4reg/python/results/ecore/*
rm -rf ecore4reg/python/results/ecore4reg/*
rm -rf ecore4reg/python/results/json/*
rm -rf ecore4reg/python/results/matches/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python create_finrep30_ecore_with_subdomains.py


