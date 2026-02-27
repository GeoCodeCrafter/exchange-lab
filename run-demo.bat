@echo off
setlocal
cd /d "%~dp0"

echo [1/3] Ingesting sample CSV...
cargo run -p exchange-lab-cli -- ingest-csv --input data/sample_events.csv --log data/raw.log --index data/raw.idx
if errorlevel 1 goto :fail

echo [2/3] Running matching engine...
cargo run -p exchange-lab-cli -- run-engine --input-log data/raw.log --input-index data/raw.idx --output-log data/engine.log --output-index data/engine.idx
if errorlevel 1 goto :fail

echo [3/3] Replaying engine journal...
cargo run -p exchange-lab-cli -- replay --log data/engine.log --index data/engine.idx --mode max-speed
if errorlevel 1 goto :fail

echo.
echo Demo completed. Output files are in the data folder.
start "" explorer "%~dp0data"
pause
exit /b 0

:fail
echo.
echo Demo failed.
pause
exit /b 1
