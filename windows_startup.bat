@echo off
echo Installing Python dependencies for Windows...
cd birds_nest
pip install --upgrade pip --quiet
python -m pip install django==5.1.3 --quiet
python -m pip install pyecore==0.15.1 --quiet
python -m pip install pytest==8.3.4 --quiet
python -m pip install pytest-xdist==3.6.1 --quiet
python -m pip install ruff==0.9.7 --quiet
python -m pip install unidecode==1.3.8 --quiet
python -m pip install pandas --quiet
python -m pip install numpy --quiet
python -m pip install requests --quiet
python -m pip install psutil --quiet
python -m pip install mssql-django pyodbc --quiet
echo.
echo Starting Django development server with auto-restart...
echo Press Ctrl+C to stop the server
echo.
:loop
python manage.py runserver --noreload
if errorlevel 1 (
    echo Server encountered an error. Restarting in 5 seconds...
    timeout /t 5 /nobreak > nul
)
goto loop