"""
Completion queue for resolving asynchronous response
"""
import datetime
from typing import List

import rx


class CompletionToken(object):
    def __init__(self, identifier, payload):
        self.identifier = identifier
        self.payload = payload


class CompletionTask(object):
    def __init__(self, identifier, observer: rx.typing.Observer, timeout=None, auto_remove=True):
        self.auto_remove = auto_remove
        self.observer = observer
        self.identifier = identifier

        if timeout:
            if isinstance(timeout, datetime.timedelta):
                self.timeout = datetime.datetime.now() + timeout
            elif isinstance(timeout, datetime.datetime):
                self.timeout = timeout
            else:
                raise TypeError()
        else:
            self.timeout = None

    def resolve(self, data):
        self.observer.on_next(data)
        self.observer.on_completed()

    def error(self, ex):
        self.observer.on_error(ex)

    def timed_out(self):
        self.error(TimeoutError(self))


class CompletionQueue(object):
    queue: List[CompletionTask]

    def __init__(self):
        self.queue = []

    def step(self, token: CompletionToken):
        for task in self.queue:
            if task.timeout:
                if task.timeout < datetime.datetime.now():
                    task.timed_out()
                    if task.auto_remove:
                        self.queue.remove(task)
                    continue

            if token.identifier == task.identifier:
                try:
                    task.resolve(token.payload)
                except Exception as ex:
                    task.error(ex)
                if task.auto_remove:
                    self.queue.remove(task)
                break

    def push(self, task: CompletionTask):
        self.queue.append(task)
