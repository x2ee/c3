# c3
c3 (compute, cache, cron)

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
