"""
RxSimex python interface
"""
import typing
from asyncio.streams import StreamReader, StreamWriter

import rx
from rx import subject
from rx import scheduler
from rx import operators

import enum
import socket
import msgpack
import numpy as np
import simex.request_internal as req
import simex.version as version

from simex.completion_queue import CompletionQueue, CompletionToken


def id_to_type(type_id):
    if type_id == 0:
        return np.float64
    elif type_id == 1:
        return np.float32
    elif type_id == 2:
        return np.int8
    elif type_id == 3:
        return np.uint8
    elif type_id == 4:
        return np.int16
    elif type_id == 5:
        return np.uint16
    elif type_id == 6:
        return np.int32
    elif type_id == 7:
        return np.uint32
    elif type_id == 8:
        return np.bool
    else:
        raise RuntimeError(f"unknown type id {type_id}")


class SimexObject(object):
    """
    Simex message common base class
    """

    def __init__(self):
        pass


class SimexSimulationState(SimexObject):
    """
    Simulation state change message
    """

    class State(enum.Enum):
        STARTED = 1
        STOPPED = 2
        ENABLED = 3
        DISABLED = 4
        REINITIALIZED = 5
        UNKNOWN = 6

    def __init__(self, message=None):
        super().__init__()
        if message is not None:
            self.state = SimexSimulationState.State.UNKNOWN
        else:
            self.state = message


class SimexBlockInfo(SimexObject):
    """
    Simex block info message class
    """

    def __init__(self, message=None):
        super().__init__()
        if message is None:
            self.path = ""
            self.sample_time = []
            self.version = []
        else:
            try:
                self.version = message[b"version"]
                self.path = message[b"path"]
                self.sample_time = [
                    message[b"sample_time"],
                    message[b"offset_time"]
                ]
            except Exception as ex:
                raise Exception("failed to parse block info", ex)


class SimexPort(SimexObject):
    """
    Simex port message class
    """
    dims: np.ndarray
    type: type
    data: np.ndarray

    def __init__(self, message=None):
        super().__init__()

        if message is None:
            self.name = ""
            self.id = -1
            self.data = None
            self.type = None
            self.dims = None
        else:
            try:
                self.name = message[b"name"]
                self.id = message[b"id"]
                try:
                    # update report does not contain dim and type_id
                    self.dim = np.array(message[b"dimensions"], dtype=int)
                    self.type = id_to_type(message[b"type_id"])
                except:
                    self.dim = None
            except Exception as ex:
                raise Exception("failed to parse port", ex)


class SimexParameter(SimexObject):
    """
    Simex parameter message class
    """

    def __init__(self, message=None):
        super().__init__()

        if message is None:
            self.name = ""
            self.data = ""
        else:
            try:
                self.name = message[b"name"]
                self.data = message[b"data"]
            except Exception as ex:
                raise Exception("failed to parse parameter", ex)


class SimexInfo(SimexObject):
    """
    Simex info message class
    """
    block_info: SimexBlockInfo
    output_ports: [SimexPort]
    input_ports: [SimexPort]
    params: [SimexParameter]

    def __init__(self, message=None):
        super().__init__()
        if message is None:
            self.block_info = None
            self.params = None
            self.input_ports = None
            self.output_ports = None
        else:
            try:
                block = message[b"block"]
                params = message[b"parameters"]
                input_ports = message[b"input_ports"]
                output_ports = message[b"output_ports"]

                self.block_info = SimexBlockInfo(block)
                self.params = [SimexParameter(x) for x in params]
                self.input_ports = [SimexPort(x) for x in input_ports]
                self.output_ports = [SimexPort(x) for x in output_ports]
            except Exception as ex:
                raise Exception("failed to parse block info object", ex)


class SimexRemote(SimexObject):
    """
    Main class for access the simex block data.
    """
    socket_writer: StreamWriter
    socket_reader: StreamReader

    def __init__(self, ip: str, port: int):
        """
        :param ip:
        :param port:
        """
        super().__init__()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ip = ip
        self.port = port
        self.socket_reader = None
        self.socket_writer = None
        self.is_connected = False
        self.unpacker = msgpack.Unpacker()
        self.cq = CompletionQueue()
        self.error_channel = subject.Subject()

        # background thread for incoming messages
        self.incoming_schduler = scheduler.NewThreadScheduler()

        # configure subscriptions
        self.input_port_updated = req.InputPortUpdateEvent(self.socket, self.cq) \
            .register() \
            .pipe(operators.map(lambda vals: [SimexPort(x) for x in vals[0]]))

        self.error_occured = req.ErrorEvent(self.socket, self.cq) \
            .register() \
            .pipe(operators.map(lambda vals: vals[0]))

        self.params_updated = req.ParameterEvent(self.socket, self.cq) \
            .register() \
            .pipe(operators.flat_map(lambda x: self.get_parameters()))

        self.simulation_state_updated = req.SimulationStateEvent(self.socket, self.cq) \
            .register() \
            .pipe(operators.map(lambda vals: SimexSimulationState(vals[0])))

    def connect(self):
        self.socket.connect((self.ip, self.port))
        self.is_connected = True
        self.incoming_schduler.schedule(self.incoming_message_task)

    def disconnect(self):
        self.socket.close()
        self.is_connected = False

    def verify_compatibility(self) -> rx.typing.Observable:
        def compare_version(info: SimexInfo):
            return info.block_info.version[0] == version.version[0] and info.block_info.version[
                2] == version.protocol_version

        return self.request_info().pipe(operators.map(compare_version))

    def get_parameters(self) -> rx.typing.Observable:
        def param(info: SimexInfo):
            return info.params

        return self.request_info().pipe(operators.map(param))

    def handle_incoming_message(self):
        # blocking
        buf = self.socket.recv(10240)
        self.unpacker.feed(buf)
        for v in self.unpacker:
            try:
                packet_identifier = v[0]
                self.cq.step(CompletionToken(packet_identifier, v[1:]))
            except Exception as ex:
                self.error_channel.on_next(ex)

    def incoming_message_task(self, schl: rx.typing.Scheduler, state=None):
        if self.is_connected:
            self.handle_incoming_message()
            schl.schedule(self.incoming_message_task)

    def request_info(self):
        if not self.is_connected:
            raise RuntimeError("socket is not connected")
        request = req.QueryInfoRequest(self.socket, self.cq)
        return request.exec().pipe(
            operators.map(lambda x: SimexInfo(x[0]))
        )

    def request_port_update(self, port_id: typing.Union[str, int], data: np.ndarray, transpose=True):
        if not self.is_connected:
            raise RuntimeError("socket is not connected")

        return req.UpdatePortRequest(self.socket, self.cq, port_id, data, transpose).exec()
