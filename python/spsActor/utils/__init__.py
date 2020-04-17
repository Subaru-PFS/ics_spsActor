import os
import time
from functools import partial

from actorcore.QThread import QThread
from pfscore.spectroIds import SpectroIds


def describe(filename):
    exposureId = os.path.splitext(filename)[0]
    if len(exposureId) != 12:
        raise ValueError(f'Invalid exposureId : {exposureId}')

    visit = int(exposureId[4:10])
    specId = int(exposureId[10])
    armNum = int(exposureId[11])

    return visit, cameraId(specId, armNum)


def cameraId(specId, arm):
    if arm in SpectroIds.validArms.keys():
        armNum = SpectroIds.validArms[arm]
    elif arm in SpectroIds.validArms.values():
        armNum = arm
    else:
        raise ValueError(f'Invalid arm : {arm}')

    return (specId - 1) * 4 + armNum


def camPerSpec(cams):
    d = dict([(int(cam[1]), []) for cam in cams])
    for cam in cams:
        d[int(cam[1])].append(cam[0])
    return d


def cmdKeys(cmdVar):
    return dict(sum([[(k.name, k) for k in reply.keywords] for reply in cmdVar.replyList], []))


def parseArgs(**kwargs):
    """ Strip given text field from rawCmd """
    args = []
    for k, v in kwargs.items():
        if v is None or v is False:
            continue
        args.append(k if v is True else f'{k}={v}')

    return args


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
