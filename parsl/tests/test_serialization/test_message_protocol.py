import logging
import pickle
import uuid

import pytest

from parsl.executors.high_throughput.message_protocol import TaskMessage
from parsl.serialize.serializer import Serializer, SerializerT


@pytest.mark.local
def test_serializer_type(serializer: SerializerT = Serializer()) -> None:
    # The function definition here forces runtime checks that the Serializer
    # confirms to the protocol definition

    assert hasattr(serializer, "serialize_task")
    assert hasattr(serializer, "serialize_result")
    assert hasattr(serializer, "deserialize_task")
    assert hasattr(serializer, "deserialize_result")


@pytest.mark.local
def test_task_message():

    task_message = TaskMessage(task_id=str(uuid.uuid4()),
                               task_buffer=b'BUFFER',
                               resource_specification={
                                   'num_nodes': 2,
                                   'ranks_per_node': 2
                               })
    foo = pickle.dumps(task_message)
    d_task_message = pickle.loads(foo)
    assert task_message.task_id == d_task_message.task_id
    assert task_message.task_buffer == task_message.task_buffer

