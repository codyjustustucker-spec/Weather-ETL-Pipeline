@echo off

py -c "import pandas, requests, yaml" >nul 2>&1

if errorlevel 1 (
    echo First-time setup: installing required packages...
    py -m pip install -r requirements.txt

    if errorlevel 1 (
        echo.
        echo Setup failed.
        pause
        exit /b 1
    )
)

echo Running Weather ETL...
py -m src.main

if errorlevel 1 (
    echo.
    echo Something went wrong.
    pause
    exit /b 1
)

echo.
echo Done.
echo Output saved to:
echo data\daily_summary\daily_summary.csv
echo data\weather.db
echo.

pause
