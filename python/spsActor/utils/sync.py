import time

from actorcore.QThread import QThread
from spsActor.utils import wait, threaded, parseArgs


class Sync(object):
    def __init__(self):
        self.cmdThd = []

    @property
    def cmdVars(self):
        return [th.cmdVar for th in self.cmdThd]

    @classmethod
    def spectrograph(cls, spsActor, specNums, cmdStr, **kwargs):
        obj = cls()
        obj.cmdThd = [EnuThread(spsActor, specNum, cmdStr, **kwargs) for specNum in specNums]
        return obj

    @classmethod
    def camera(cls, actor, cams, cmdStr, **kwargs):
        obj = cls()
        obj.cmdThd = [XcuThread(actor, cam, cmdStr, **kwargs) for cam in cams]
        return obj

    def process(self, cmd):
        self.call(cmd)
        self.sync()
        return self.examAndExit()

    def call(self, cmd):
        for th in self.cmdThd:
            th.call(cmd)

    def sync(self):
        while None in self.cmdVars:
            wait()

    def examAndExit(self):
        cams = []
        for th in self.cmdThd:
            cams.extend(th.exposable)
            th.exit()
        delattr(self, 'cmdThd')


class CmdThread(QThread):
    def __init__(self, spsActor, actorName, cmdStr, **kwargs):
        cmdStr = ' '.join([cmdStr] + parseArgs(**kwargs))
        self.cmdVar = None
        self.actorName = actorName
        self.cmdStr = cmdStr
        QThread.__init__(self, spsActor, str(time.time()))
        QThread.start(self)

    @threaded
    def call(self, cmd):
        self.cmdVar = self.actor.safeCall(actor=self.actorName, cmdStr=self.cmdStr, forUserCmd=cmd)


class EnuThread(CmdThread):
    def __init__(self, spsActor, specNum, cmdStr, **kwargs):
        self.specNum = specNum
        actorName = f'enu_sm{specNum}'
        CmdThread.__init__(self, spsActor, actorName, cmdStr, **kwargs)

    @property
    def exposable(self):
        cams = [f'{arm}{self.specNum}' for arm in ['b', 'r', 'n']] if not self.cmdVar.didFail else []
        return cams


class XcuThread(CmdThread):
    def __init__(self, spsActor, cam, cmdStr, **kwargs):
        self.cam = cam
        actorName = f'xcu_{cam}'
        CmdThread.__init__(self, spsActor, actorName, cmdStr, **kwargs)

    @property
    def exposable(self):
        cams = [self.cam] if not self.cmdVar.didFail else []
        return cams
