import time
from datetime import datetime as dt
from functools import partial

from actorcore.QThread import QThread


def fromisoformat(date, fmt='%Y-%m-%dT%H:%M:%S.%f'):
    return dt.strptime(date, fmt)


def wait(ti=0.001):
    time.sleep(ti)


def putMsg(func):
    def wrapper(self, cmd, *args, **kwargs):
        self.putMsg(partial(func, self, cmd, *args, **kwargs))

    return wrapper


def putMsg2(func):
    def wrapper(self, cmd, *args, **kwargs):
        thr = QThread(self.actor, str(time.time()))
        thr.start()
        thr.putMsg(partial(func, self, cmd, *args, **kwargs))
        thr.exitASAP = True

    return wrapper


def threaded(func):
    @putMsg
    def wrapper(self, cmd, *args, **kwargs):
        try:
            return func(self, cmd, *args, **kwargs)
        except Exception as e:
            cmd.fail('text=%s' % self.actor.strTraceback(e))

    return wrapper


def singleShot(func):
    @putMsg2
    def wrapper(self, cmd, *args, **kwargs):
        try:
            return func(self, cmd, *args, **kwargs)
        except Exception as e:
            cmd.fail('text=%s' % self.actor.strTraceback(e))

    return wrapper
