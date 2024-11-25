from typing import Optional, Union
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

from x2.c3.hwm import encode_base64, ensure_bytes

PUBLIC_KEY_LENGTH = len(Ed25519PrivateKey.generate().public_key().public_bytes_raw())

class EntitySession:
    private_key: Ed25519PrivateKey
    host_pk: Optional["EntityPubKey"]

    def __init__(self, host_pk: Union[None,str,bytes,Ed25519PublicKey,"EntityPubKey"]=None) -> None:
        self.private_key = Ed25519PrivateKey.generate()
        self.host_pk = EntityPubKey.ensure(host_pk)

    def sign(self, data: Union[str,bytes]) -> bytes:
        return self.private_key.sign(ensure_bytes(data))

    def public_key(self) -> "EntityPubKey":
        return EntityPubKey(self.private_key.public_key())


class EntityPubKey:
    public_key: Ed25519PublicKey

    @staticmethod
    def ensure(input: Union[None, str, bytes, Ed25519PublicKey, "EntityPubKey"]) -> Optional["EntityPubKey"]:
        if input is None or isinstance(input, EntityPubKey):
            return input
        return EntityPubKey(input)
    
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
