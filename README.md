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

Apps shares responsibilities and communicate with each other via http.


Initially no authorization will be done but services will work on localhost only. Auth tokens have to be employed when. 

### AnyApp

* `/status/`
  * health of the node
  * worker discovery
  * data shard discovery
* `/shutdown/` - shutdown itself

### LeaderApp


Leader could be in two modes: 
  * cluster leader 
  * slave leader
  * 
* accepting external/internal traffic and dispatch it `/dnode/`
* `pt:scheduler`
  * running scheduler
* `pt:cluster`
  * monitoring health of cluster and starting workers and storage nodes, maintain cluster directory.
  * schedule dnode execs on workers
* could cary out duty of storage app & worker app



### StorageApp
* store and retrieve data for workers `/data`

### WorkerApp
* run dnode execs on request of scheduler `/exec`
* store logs


## API Endpoints


`/dnode/...?a=b&c=5`

high level access to data. Can trigger a lazy or forceful execution of dnode resulting in the cache update.

path - ...dnode path..

variables - ?var1=1&var2=zzz
  * GET - retrieve data without side effect.
  * POST - retrieve data refreshing it if necessary.
  * PUT - store or overwrite data

On POST calculation will be forced if `ForceCalc=true` header is provided, also `Interval` header could be provided 
to ensure that value returned was calculated within given interval. By default `Interval` associated with given caching rules.

`as_of_date` variable has special meaning for caching layer.  It must to be present in calculation method signature as first argument. 
If calculation does not support `as_of_date`, for example calculation downloads data that changes over time. It is good idea to explicitly fail 
on any date other than today. In this case to retrieve `keep` cache should be used and previous versions of the data will be stored according 
to cache interval.


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

