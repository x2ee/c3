# c3
c3 (compute, cache, cron)

[![Build Status](https://dev.azure.com/sekash/Public/_apis/build/status%2Fx2ee.c3?branchName=main)](https://dev.azure.com/sekash/Public/_build/latest?definitionId=8&branchName=main)

## Developer setup

Create environment. That step could be done with conda or venv.

With conda:

```bash
conda create -y -n c3 python=3.9
conda activate c3
```

With venv:
```bash
python -m venv build/venv
. build/venv/bin/activate
```

Then install all dependencies:
```bash
pip install -e .[dev]
```

Then run all tests:
```bash
pytest
```

Inspect [coverage](htmlcov/index.html).
