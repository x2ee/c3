from typing import Optional, Union
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

from x2.c3.hwm import encode_base64, ensure_bytes


class WorkerSession:
    private_key: Ed25519PrivateKey
    host_pk: Optional["WorkerSessionPubKey"]

    def __init__(self, host_pk: Union[None,str,bytes,Ed25519PublicKey,"WorkerSessionPubKey"]=None) -> None:
        self.private_key = Ed25519PrivateKey.generate()
        self.host_pk = WorkerSessionPubKey.ensure(host_pk)

    def sign(self, data: Union[str,bytes]) -> bytes:
        return self.private_key.sign(ensure_bytes(data))

    def public_key(self) -> "WorkerSessionPubKey":
        return WorkerSessionPubKey(self.private_key.public_key())


class WorkerSessionPubKey:
    public_key: Ed25519PublicKey

    @staticmethod
    def ensure(input: Union[None, str, bytes, Ed25519PublicKey, "WorkerSessionPubKey"]) -> Optional["WorkerSessionPubKey"]:
        if input is None or isinstance(input, WorkerSessionPubKey):
            return input
        return WorkerSessionPubKey(input)
    
    def __init__(self, public_key: Union[str,bytes,Ed25519PublicKey]) -> None:
        if isinstance(public_key, Ed25519PublicKey):
            self.public_key = public_key
        else:
            self.public_key = Ed25519PublicKey.from_public_bytes(ensure_bytes(public_key))

    def verify(self, data: Union[str, bytes], signature: Union[str,bytes]) -> bool:
        try:
            self.public_key.verify(ensure_bytes(signature), ensure_bytes(data))
        except InvalidSignature:
            return False
        return True
    
    def __str__(self) -> str:
        bb = self.public_key.public_bytes_raw()
        return encode_base64(bb)
