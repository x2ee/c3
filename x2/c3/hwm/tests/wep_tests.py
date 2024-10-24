import pytest
from datetime import datetime, timedelta
from x2.c3.hwm.wep import WorkOrder, WorkOrderCommand

@pytest.mark.debug
def test_work_order_creation():
    # Test creating a WorkOrder with default values
    work_order = WorkOrder(command=WorkOrderCommand.SHUTDOWN)
    assert work_order.command == WorkOrderCommand.SHUTDOWN
    assert isinstance(work_order.timestamp, datetime)
    assert work_order.payload == b""

    from_bytes_work_order = WorkOrder.from_bytes(bytes(work_order))
    assert from_bytes_work_order.command == work_order.command
    assert from_bytes_work_order.timestamp <= work_order.timestamp
    assert from_bytes_work_order.timestamp+timedelta(milliseconds=1) >= work_order.timestamp
    assert from_bytes_work_order.payload == work_order.payload

    from_bytes_work_order2 = WorkOrder.from_bytes(bytes(from_bytes_work_order))
    assert from_bytes_work_order2.command == from_bytes_work_order.command
    assert from_bytes_work_order2.timestamp == from_bytes_work_order.timestamp
    assert from_bytes_work_order2.payload == from_bytes_work_order.payload

@pytest.mark.xfail(raises=AssertionError, strict=True)
def test_work_order_wrong_size_fixed_payload():
    work_order = WorkOrder(command=WorkOrderCommand._FIXED_PAYLOAD, payload=b"hello")
    bytes(work_order)

def test_work_order_fixed_payload():
    work_order = WorkOrder(command=WorkOrderCommand._FIXED_PAYLOAD, payload=b"h" * 10)

    from_bytes_work_order = WorkOrder.from_bytes(bytes(work_order))
    assert from_bytes_work_order.command == work_order.command
    assert from_bytes_work_order.timestamp <= work_order.timestamp
    assert from_bytes_work_order.timestamp+timedelta(milliseconds=1) >= work_order.timestamp
    assert from_bytes_work_order.payload == work_order.payload

    from_bytes_work_order2 = WorkOrder.from_bytes(bytes(from_bytes_work_order))
    assert from_bytes_work_order2.command == from_bytes_work_order.command
    assert from_bytes_work_order2.timestamp == from_bytes_work_order.timestamp
    assert from_bytes_work_order2.payload == from_bytes_work_order.payload

def test_work_order_variable_payload():
    work_order = WorkOrder(command=WorkOrderCommand._VARIABLE_PAYLOAD)

    from_bytes_work_order = WorkOrder.from_bytes(bytes(work_order))
    assert from_bytes_work_order.command == work_order.command
    assert from_bytes_work_order.timestamp <= work_order.timestamp
    assert from_bytes_work_order.timestamp+timedelta(milliseconds=1) >= work_order.timestamp
    assert from_bytes_work_order.payload == work_order.payload

    from_bytes_work_order2 = WorkOrder.from_bytes(bytes(from_bytes_work_order))
    assert from_bytes_work_order2.command == from_bytes_work_order.command
    assert from_bytes_work_order2.timestamp == from_bytes_work_order.timestamp
    assert from_bytes_work_order2.payload == from_bytes_work_order.payload
    
    for sz in range(0, 51, 25):
        work_order = WorkOrder(command=WorkOrderCommand._VARIABLE_PAYLOAD, payload=b"h" * sz)

        from_bytes_work_order = WorkOrder.from_bytes(bytes(work_order))
        assert from_bytes_work_order.command == work_order.command
        assert from_bytes_work_order.timestamp <= work_order.timestamp
        assert from_bytes_work_order.timestamp+timedelta(milliseconds=1) >= work_order.timestamp
        assert from_bytes_work_order.payload == work_order.payload

        from_bytes_work_order2 = WorkOrder.from_bytes(bytes(from_bytes_work_order))
        assert from_bytes_work_order2.command == from_bytes_work_order.command
        assert from_bytes_work_order2.timestamp == from_bytes_work_order.timestamp
        assert from_bytes_work_order2.payload == from_bytes_work_order.payload
