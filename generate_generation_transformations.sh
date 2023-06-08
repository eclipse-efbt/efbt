#!/bin/bash

# clear results script

rm -rf ecore4reg/python/results/ecore/*
rm -rf ecore4reg/python/results/ecore4reg/*
rm -rf ecore4reg/python/results/json/*
rm -rf ecore4reg/python/results/matches/*
rm -rf ecore4reg/python/results/xcore/*
rm -rf ecore4reg/python/results/sdd/*
rm -rf ecore4reg/python/results/xmi/*

cd ecore4reg/python/src/
pip install pyecore
pip install unidecode
python generate_generation_rules.py
