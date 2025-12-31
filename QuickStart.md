# Quick Start Guide

## Installation

### Windows / PowerShell

#### 1) Create and activate a virtual environment

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
pip install -r .\requirements.txt
2) Add the Discharge Summary template
Create a local templates folder and copy DS_SampleC1.xml into it:

powershell
Copy code
New-Item -ItemType Directory -Force .\templates | Out-Null
Copy-Item -Force "C:\Path\To\DS_SampleC1.xml" .\templates\DS_SampleC1.xml
3) Generate Discharge Summaries
Create a timestamped output folder and generate 10 files:

powershell
Copy code
$run = "output_run_" + (Get-Date -Format "yyyyMMdd_HHmmss")
New-Item -ItemType Directory -Force -Path $run | Out-Null

python .\tools\ds_from_template_generate.py `
  --template .\templates\DS_SampleC1.xml `
  --outdir $run `
  --count 10
Optional: deterministic generation (useful for debugging)
Use a seed to produce repeatable outputs:

powershell
Copy code
python .\tools\ds_from_template_generate.py `
  --template .\templates\DS_SampleC1.xml `
  --outdir $run `
  --count 10 `
  --seed 123
Output
Generated files are written to your chosen output folder (e.g. $run):

ds_001.xml

ds_002.xml

…

ds_010.xml
