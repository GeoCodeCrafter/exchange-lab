@echo off
setlocal
cd /d "%~dp0"

if not exist "data\engine.log" goto :prepare
if not exist "data\engine.idx" goto :prepare
goto :serve

:prepare
echo [prep] Building a local journal from the bundled sample feed...
cargo run -p exchange-lab-cli -- ingest-csv --input data/sample_events.csv --log data/raw.log --index data/raw.idx
if errorlevel 1 goto :fail
cargo run -p exchange-lab-cli -- run-engine --input-log data/raw.log --input-index data/raw.idx --output-log data/engine.log --output-index data/engine.idx
if errorlevel 1 goto :fail
cargo run -p exchange-lab-cli -- snapshot --log data/engine.log --index data/engine.idx --output data/book.snapshot
if errorlevel 1 goto :fail

:serve
echo [live] Starting dashboard on http://127.0.0.1:8080
start "" cmd /c "ping 127.0.0.1 -n 3 >nul && start http://127.0.0.1:8080/"
cargo run -p exchange-lab-cli -- serve --log data/engine.log --index data/engine.idx --snapshot data/book.snapshot --grpc-addr 127.0.0.1:50051 --http-addr 127.0.0.1:8080
if errorlevel 1 goto :fail
exit /b 0

:fail
echo.
echo Live launcher failed.
pause
exit /b 1
