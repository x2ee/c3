import pytest
from x2.c3.hwm.session import WorkerSession, WorkerSessionPubKey, encode_base64
from os import urandom

@pytest.mark.debug
def test_worker_session():
    challenge = urandom(64)
    host = WorkerSession()
    assert host.host_pk is None
    wk1 = WorkerSession(host_pk=str(host.public_key()))
    wk2 = WorkerSession(host_pk=str(host.public_key()))
    s1 = wk1.sign(challenge)
    assert s1 == wk1.sign(encode_base64(challenge))
    assert False == wk2.public_key().verify(challenge, s1)
    assert wk1.public_key().verify(encode_base64(challenge), s1)
    assert wk1.public_key().verify(challenge, encode_base64(s1))
    assert wk1.public_key().verify(challenge, s1)
    assert WorkerSessionPubKey.ensure(str(wk1.public_key())).verify(
        encode_base64(challenge), encode_base64(s1)
    )