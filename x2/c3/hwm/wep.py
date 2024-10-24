# WEP Stands for Worker End Point
# worker in this context is a process that is running on a host and accessible through a http
# endpoint. The worker is identified by a public key and a URL host (hostname and port).
# WorkerHost is that manage other workers either locally, in a container or in a remote host.


from base64 import b64decode, b64encode
from datetime import datetime
from enum import Enum
import json
from typing import List, Optional
from tornado.httpclient import AsyncHTTPClient

from pydantic import Field

from x2.c3.periodic import DT_BYTES_LENGTH, dt_from_bytes, dt_to_bytes
from x2.c3 import JsonBase
from x2.c3.hwm.session import WorkerSessionPubKey
from x2.c3.hwm import MAX_PORT


class UrlHost(JsonBase):
    hostname: str = Field(title="Hostname or IP address")
    port: int = Field(title="Port number", ge=1, le=MAX_PORT)

    @staticmethod
    def from_bytes(data: bytes):
        port = int.from_bytes(data[:2], "big")
        hostname = data[2:].decode()
        return UrlHost(hostname=hostname, port=port)
    
    def __bytes__(self):
        hostname_b = self.hostname.encode()
        port_b = self.port.to_bytes(2, "big")
        return port_b + hostname_b
    
    async def fetch_json(self, path):
        """Fetch json from path."""
        url = f"http://{self.hostname}:{self.port}/{path}"
        response = await AsyncHTTPClient().fetch(url)
        return json.loads(response.body)


class ProcessId(JsonBase):
    pid: Optional[int] = Field(
        title="Process ID. Expected to be provided on OS process based host.", default=None
    )
    container_id: Optional[str] = Field(
        title="Container ID. Expected to be provided on Docker/Pacman based host.", default=None
    )

class WorkerId(JsonBase):
    public_key: str
    url: UrlHost

    @staticmethod
    def from_bytes(data: bytes):
        pk_b = data[:32]
        url_b = data[32:]
        return WorkerId(public_key=b64encode(pk_b).decode(), url=UrlHost.from_bytes(url_b))

    @staticmethod
    def from_str(data: str):
        return WorkerId.from_bytes(b64decode(data))
    
    def __bytes__(self):
        pk_b = b64decode(self.public_key)
        assert len(pk_b) == 32
        return pk_b + self.url.__bytes__()
    
    def __str__(self):
        return b64encode(self.__bytes__()).decode()
    
    def get_pk(self) -> WorkerSessionPubKey:
        return WorkerSessionPubKey(self.public_key)


class Worker(JsonBase):
    wid: WorkerId
    process_id: Optional[ProcessId]
    updated: datetime
    tags: List[str]
    host_signature: Optional[str] = Field(
        title="`bytes(worker.wid)` signed by host. Populated only host after worker listed in directory", default=None
    )


class WorkerInitiationRequest(JsonBase):
    """
    Request to initiate a worker session. Passed through stdin to the worker process.
    """
    worker_host: Worker
    challenge: str


class WorkerBindResponse(JsonBase):
    """
    Response from the worker process to the host process. Passed through http request to the host `url`.
    """
    worker: Worker
    challenge_sig: str


class HostAcknowledge(JsonBase):
    """
    Response from the worker host to the worker process. Passed through http at worker's `url`.
    """
    worker_host: Worker
    worker: Worker
    listed: datetime


class WorkerHost(Worker):
    workers: List[Worker]


class WorkOrderCommand(Enum):
    SHUTDOWN = (1)
    _VARIABLE_PAYLOAD = (250, True)
    _FIXED_PAYLOAD = (251, True, 10)

    def __init__(self, index: int, has_payload: bool=False, payload_size: Optional[int] = None):
        self.index = index
        type(self)._member_map_[f'{index=}'] = self
        self.has_payload = has_payload
        self.payload_size = payload_size

    @classmethod
    def from_index(cls, index: int):
        return cls._member_map_[f'{index=}']
    
    def __bytes__(self):
        return self.index.to_bytes(1, "big")


class WorkOrder(JsonBase):
    """
    Host command to be executed by the worker.
    """
    command: WorkOrderCommand
    timestamp: datetime = Field(title="Timestamp of the work order", default_factory=datetime.now)
    payload: bytes = Field(title="Payload of the work order", default=b"")

    @staticmethod
    def from_bytes(data: bytes):
        command = WorkOrderCommand.from_index(data[0])
        offset = 1
        end = offset + DT_BYTES_LENGTH
        timestamp = dt_from_bytes(data[offset:end])
        payload = b''
        if command.has_payload:
            if command.payload_size is None:
                offset,end = end, end + 2
                payload_size = int.from_bytes(data[offset:end], 'big')
                offset,end = end, end + payload_size
                payload = data[offset:end]
                assert len(payload) == payload_size
            else:
                payload = data[end:]
                assert len(payload) == command.payload_size
        return WorkOrder(command=command, timestamp=timestamp, payload=payload)

    def __bytes__(self):
        timestamp_b = dt_to_bytes(self.timestamp)
        payload_b = b''
        if self.command.has_payload:
            if self.command.payload_size is None:
                payload_size = len(self.payload)
                assert payload_size < 2 ** 16
                payload_b = payload_size.to_bytes(2, "big") + self.payload
            else:
                assert len(self.payload) == self.command.payload_size
                payload_b = self.payload
        return bytes(self.command) + timestamp_b + payload_b