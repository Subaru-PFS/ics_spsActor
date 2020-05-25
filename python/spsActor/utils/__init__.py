import time
from datetime import datetime as dt
from functools import partial

from actorcore.QThread import QThread


def fromisoformat(date, fmt='%Y-%m-%dT%H:%M:%S.%f'):
    return dt.strptime(date, fmt)


def camPerSpec(cams):
    d = dict([(int(cam[1]), []) for cam in cams])
    for cam in cams:
        d[int(cam[1])].append(cam[0])
    return d


def cmdKeys(cmdVar):
    return dict(sum([[(k.name, k) for k in reply.keywords] for reply in cmdVar.replyList], []))


def parse(cmdStr, **kwargs):
    """ Strip given text field from rawCmd """
    args = []
    for k, v in kwargs.items():
        if v is None or v is False:
            continue
        args.append(k if v is True else f'{k}={v}')

    return ' '.join([cmdStr.strip()] + args)


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
