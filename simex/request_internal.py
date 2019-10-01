"""
1. Initiate request and return the response as an observable.
2. Subscribe to broadcast messages.
"""

import typing
import socket
from typing import List, Any

import msgpack
import datetime
import numpy as np
import rx
from rx import subject
import asyncio

from simex.completion_queue import CompletionTask, CompletionQueue


class BaseRequest(object):
    def __init__(self, _socket, completion_queue=None):
        self._socket = _socket
        self.completion_queue: CompletionQueue = completion_queue

    def _get_completion_task(self, observer) -> CompletionTask:
        pass

    def _send_request(self, content: bytes):
        self._socket.send(content)


class BaseReqRepRequest(BaseRequest):
    def __init__(self, _socket, completion_queue):
        super().__init__(_socket, completion_queue)

    def _pack(self) -> bytes:
        pass

    def exec(self) -> rx.Observable:
        def subscribe(observer, sc=None):
            if self.completion_queue:
                completion_task = self._get_completion_task(observer)
                if completion_task is not None:
                    cq = self.completion_queue
                    cq.push(completion_task)
            content = self._pack()
            self._send_request(content)
            return lambda: None

        return rx.create(subscribe)


class BaseSubscribeRequest(BaseRequest):
    def __init__(self, _socket, completion_queue, sub=subject.Subject()):
        super().__init__(_socket, completion_queue)
        self.registered = False
        self.subject = sub

    def register(self) -> rx.Observable:
        if not self.registered:
            self.completion_queue.push(self._get_completion_task(self.subject))
            self.registered = True
        return self.subject


class QueryInfoRequest(BaseReqRepRequest):
    def __init__(self, _socket, completion_queue, timeout=datetime.timedelta(seconds=5)):
        self.timeout = timeout
        super().__init__(_socket, completion_queue)

    def _get_completion_task(self, observer) -> CompletionTask:
        return CompletionTask(b"resp_info", observer, timeout=self.timeout)

    def _pack(self) -> bytes:
        return msgpack.packb([b"query_info"])


class UpdatePortRequest(BaseReqRepRequest):

    def __init__(self, _socket, completion_queue, port_identifier: typing.Union[int, str], data: np.ndarray,
                 transpose=True, timeout=datetime.timedelta(seconds=5)):
        self.timeout = timeout
        self.completion_queue = completion_queue
        self.transpose = transpose
        self.data = data.T if transpose else data
        self.port_identifier = port_identifier
        super().__init__(_socket, completion_queue)

    def _pack(self) -> bytes:
        return msgpack.packb([
            b"value",
            [
                self.port_identifier,
                bytes(self.data.data)
            ]
        ])

    def _get_completion_task(self, observer) -> CompletionTask:
        return CompletionTask(b"value_updated", observer, self.timeout)


class InputPortUpdateEvent(BaseSubscribeRequest):
    def __init__(self, _socket, completion_queue, sub=subject.Subject()):
        super().__init__(_socket, completion_queue, sub)

    def _get_completion_task(self, observer) -> CompletionTask:
        return CompletionTask(b"port_values", observer, auto_remove=False)


class ErrorEvent(BaseSubscribeRequest):
    def __init__(self, _socket, completion_queue, sub=subject.Subject()):
        super().__init__(_socket, completion_queue, sub)

    def _get_completion_task(self, observer) -> CompletionTask:
        return CompletionTask(b"error", observer, auto_remove=False)


class ParameterEvent(BaseSubscribeRequest):
    def __init__(self, _socket, completion_queue, sub=subject.Subject()):
        super().__init__(_socket, completion_queue, sub)

    def _get_completion_task(self, observer) -> CompletionTask:
        return CompletionTask(b"param_updated", observer, auto_remove=False)


class SimulationStateEvent(BaseSubscribeRequest):
    def __init__(self, _socket, completion_queue, sub=subject.Subject()):
        super().__init__(_socket, completion_queue, sub)

    def _get_completion_task(self, observer) -> CompletionTask:
        return CompletionTask(b"state_updated", observer, auto_remove=False)
