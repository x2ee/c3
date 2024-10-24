# HostWorkerModel (HWM)

Server App Built build out of services.

Service contain things that necessary for it to function:

* set of routes
* set of scheduled tasks
* set of databases it maintains

WORKER/HOST init procedure:

   1. HOST sends `WorkerInitiationRequest` to newly started WORKER
      process in it's `stdin`
   2. WORKER responds with `WorkerBindResponse` to HOST endpoint
   3. HOST responds with `HostAcknowledge` to WORKER endpoint
   4. HOST monitor WORKER continuously thru /status/ API call

## WorkerService

Routes:
  * GET `/status/`
    * state of worker INIT/READY/PENDING_SHUTDOWN/DOWN
    * public key - app public key. Used to sign messages from app to authenticate them. Key is generated when app starts
    * host signature. HostApp sign public key of the app immediately after start as acknowledgement that App was started by host and can be trusted in cluster
    * other services status info like:
      * health of the node
      * node worker discovery db
      * data shard discovery db
  * POST `/work_order/` - shutdown request authorized/signed by WorkerHost.

## WorkerHostService
Routes:

  * `/workers/`
    * most recent

* `db:

HostApp maintains and autorize : 
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

 