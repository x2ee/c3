# Server Integration Model


## AnyApp

* `/status/`
  * public key - app public key. Used to sign messages from app to authenticate them. Key is generated when app starts
  * host signature. HostApp sign public key of the app immediately after start as acknowledgement that App was started by host and can be trusted in cluster
  * health of the node
  * node worker discovery db
  * data shard discovery db
* `/shutdown/` - shutdown itself

## HostApp

* `/workers/`
  * local workers

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

