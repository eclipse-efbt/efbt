#!/bin/bash
sudo apt-get update -y
sudo apt-get install mdbtools sqlite3 -y
cd birds_nest
pip install --upgrade pip --quiet
python -m pip install django==5.1.3 --quiet
python -m pip install pyecore==0.15.1 --quiet
python -m pip install pytest==8.3.4 --quiet
python -m pip install pytest-xdist==3.6.1 --quiet
python -m pip install pandas==2.3.0 --quiet
python -m pip install numpy --quiet
python -m pip install requests --quiet
python -m pip install psutil --quiet

# python pybirdai/utils/datapoint_test_run/run_tests.py --uv "False" --config-file "tests/configuration_file_tests.json"

# Ensure database migrations are applied
python manage.py migrate --run-syncdb

# Create superuser for Django admin access (development only)
python manage.py ensure_superuser

while true; do python manage.py runserver --noreload; done
