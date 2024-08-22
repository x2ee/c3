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

## Apps

Apps shares responsibilities and communicate with eachother via http. Calls should be authorized with ACL, tokens and crypto signatures.

### AnyApp

* `/status/`
  * health of the node
  * worker discovery
  * data shard discovery
* `/shutdown/` - shutdown itself
* `/ps/` - show other apps on the same host
* `/kill/` - kill unresponsive app on the same host

### LeaderApp

* accepting external/internal traffic and dispatch it `/dnode/`
* `pt:scheduler`
  * running scheduler
* `pt:cluster`
  * monitoring health of cluster and starting workers and storage nodes
  * schedule dnode execs on workers
* could cary out duty of storage app & worker app

### StorageApp
* store and retrieve data for workers `/data`

### WorkerApp
* run dnode execs on request of scheduler `/exec`


## API Endpoints


`/dnode/...?a=b&c=5`

high level access to data. Can trigger a lazy or forceful execution of dnode resulting in the cache update.

path - ...dnode path..

variables - ?var1=1&var2=zzz

GET - retrieve data without side effect. 
POST - retrieve data refreshing it if necessary.
PUT - store or overwrite data

`/status/`

GET

status of the server.  

* load metrics
* running tasks and times
* scheduler next task
* current address book

`/exec/...dnode path..?a=b&c=5`

POST - run task send data to storage node and the report it to orchestrator

`/data/...dnode path..?a=b&c=5`

low level API to store and retrieve data

GET - get data 
PUT - set data
