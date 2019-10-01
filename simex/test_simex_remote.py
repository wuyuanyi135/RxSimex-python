from unittest import TestCase

import numpy as np

from simex.simex_remote import SimexRemote, SimexInfo
from rx import scheduler, operators
from rx.scheduler import eventloop

import asyncio


class TestSimexRemote(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.instance = SimexRemote("127.0.0.1", 12305)
        self.instance.connect()
        self.loop = asyncio.new_event_loop()
        self.executor = eventloop.AsyncIOThreadSafeScheduler(self.loop)

    def test_request_info(self):
        def _subscribe(x: SimexInfo = None):
            self.loop.stop()
            print(x.__dict__)

        def _err(x=None):
            self.loop.stop()
            self.fail(x)

        self.instance.request_info().pipe(operators.observe_on(self.executor)).subscribe(_subscribe, _err)

        self.loop.run_forever()

    def test_update_port(self):
        def _subscribe(x: SimexInfo = None):
            self.loop.stop()

        def _err(x=None):
            self.loop.stop()
            self.fail(x)

        self.instance\
            .request_port_update(0, np.asarray(13, dtype=np.float64), False)\
            .pipe(operators.observe_on(self.executor))\
            .subscribe(_subscribe, _err)
        self.loop.run_forever()

    def test_input_port_updated_subject(self):
        def _subscribe(x = None):
            print(x)
            self.loop.stop()

        def _err(x=None):
            self.loop.stop()
            self.fail(x)
        self.instance.input_port_updated\
            .pipe(operators.observe_on(self.executor))\
            .subscribe(_subscribe, _err)
        self.loop.run_forever()

    def test_param_updated_subject(self):
        def _subscribe(x = None):
            print(x)
            self.loop.stop()

        def _err(x=None):
            self.loop.stop()
            self.fail(x)
        self.instance.params_updated\
            .pipe(operators.observe_on(self.executor))\
            .subscribe(_subscribe, _err)

        print("Please change a parameter")
        self.loop.run_forever()

    def test_state_update(self):
        def _subscribe(x = None):
            print(x)
            self.loop.stop()

        def _err(x=None):
            self.loop.stop()
            self.fail(x)
        self.instance.simulation_state_updated\
            .pipe(operators.observe_on(self.executor))\
            .subscribe(_subscribe, _err)

        print("Please stop simulation")
        self.loop.run_forever()
    def tearDown(self) -> None:
        self.instance.disconnect()
        super().tearDown()
