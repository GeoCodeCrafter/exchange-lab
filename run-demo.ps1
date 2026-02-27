$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

Write-Host '[1/3] Ingesting sample CSV...'
cargo run -p exchange-lab-cli -- ingest-csv --input data/sample_events.csv --log data/raw.log --index data/raw.idx

Write-Host '[2/3] Running matching engine...'
cargo run -p exchange-lab-cli -- run-engine --input-log data/raw.log --input-index data/raw.idx --output-log data/engine.log --output-index data/engine.idx

Write-Host '[3/3] Replaying engine journal...'
cargo run -p exchange-lab-cli -- replay --log data/engine.log --index data/engine.idx --mode max-speed

Write-Host ''
Write-Host 'Demo completed. Output files are in the data folder.'
Start-Process explorer.exe (Join-Path $PSScriptRoot 'data')
