#!/bin/bash
sudo apt-get update
sudo apt-get install mdbtools
sudo chmod -R u+w birds_nest/
sudo chown www-data:www-data birds_nest/
cd birds_nest
pip install --upgrade pip --quiet
python -m pip install django==5.1.3 --quiet
python -m pip install pyecore==0.15.1 --quiet
python -m pip install pytest==8.3.4 --quiet
python -m pip install pytest-xdist==3.6.1 --quiet
python -m pip install ruff==0.9.7 --quiet
python -m pip install unidecode==1.3.8 --quiet
python -m pip install pandas --quiet

# python pybirdai/utils/datapoint_test_run/run_tests.py --uv "False" --config-file "tests/configuration_file_tests.json"
while true; do python manage.py runserver --noreload; done
