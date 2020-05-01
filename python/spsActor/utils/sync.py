import time

from actorcore.QThread import QThread
from spsActor.utils import wait, threaded


class Sync(object):
    """ Placeholder to synchronise multiple command thread. """

    def __init__(self):
        self.cmdThd = []

    def __del__(self):
        self.exit()

    @property
    def finished(self):
        return all([th.finished for th in self.cmdThd])

    @classmethod
    def slit(cls, spsActor, specNums, cmdHead, **kwargs):
        """ Create slit command thread from specNums. """
        obj = cls()
        obj.cmdThd = [Slit(spsActor, specNum, cmdHead, **kwargs) for specNum in specNums]
        return obj

    @classmethod
    def iis(cls, spsActor, specNums, cmdHead, **kwargs):
        """ Create slit command thread from specNums. """
        obj = cls()
        obj.cmdThd = [Iis(spsActor, specNum, cmdHead, **kwargs) for specNum in specNums]
        return obj

    @classmethod
    def ccdMotors(cls, actor, cams, cmdHead, **kwargs):
        """ Create ccdMotors command thread from list of camera. """
        cmdStr = f'motors {cmdHead}'.strip() if 'motors' not in cmdHead else cmdHead
        obj = cls()
        obj.cmdThd = [CcdMotors(actor, cam, cmdStr, **kwargs) for cam in cams]
        return obj

    def process(self, cmd):
        """ Call, synchronise and handle results. """
        self.call(cmd)
        self.sync()
        return self.examinate()

    def call(self, cmd):
        """ Call for each command thread. """
        for th in self.cmdThd:
            th.call(cmd)

    def sync(self):
        """ Wait for command thread to be finished. """
        while not self.finished:
            wait()

    def examinate(self):
        """ Retrieve list of exposable camera. """
        return set(sum([th.exposable for th in self.cmdThd], []))

    def exit(self):
        """ Kill all command threads before exiting. """
        for th in self.cmdThd:
            th.exit()


class CmdThread(QThread):
    """ Placeholder to a handle a single command thread. """

    def __init__(self, spsActor, actorName, cmdStr, timeLim=60, **kwargs):
        self.kwargs = kwargs
        self.cmdVar = None
        self.cancelled = False
        self.actorName = actorName
        self.cmdStr = cmdStr
        self.timeLim = timeLim
        QThread.__init__(self, spsActor, str(time.time()))
        QThread.start(self)

    @property
    def finished(self):
        return self.cancelled or self.cmdVar is not None

    @property
    def failed(self):
        return self.cancelled or self.cmdVar.didFail

    @property
    def cams(self):
        return []

    @property
    def exposable(self):
        cams = [] if self.failed else self.cams
        return cams

    @property
    def keyVarDict(self):
        return self.actor.models[self.actorName].keyVarDict

    @threaded
    def call(self, cmd):
        """ Execute precheck, cancel if an exception is raised, if not call command in the thread. """
        try:
            self.precheck()
            self.cmdVar = self.actor.safeCall(cmd, actor=self.actorName, cmdStr=self.cmdStr, timeLim=self.timeLim,
                                              **self.kwargs)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))
            self.cancel()

    def precheck(self):
        """ Condition(s) to be checked before firing the command. """
        pass

    def cancel(self):
        """ Cancel the command. """
        self.cancelled = True


class EnuThread(CmdThread):
    """ Placeholder to a handle enu command thread. """

    def __init__(self, spsActor, specNum, cmdStr, timeLim=60, **kwargs):
        self.specNum = specNum
        actorName = f'enu_sm{specNum}'
        spsActor.requireModels([actorName])
        CmdThread.__init__(self, spsActor, actorName, cmdStr, timeLim=timeLim, **kwargs)

    @property
    def cams(self):
        return [f'{arm}{self.specNum}' for arm in ['b', 'r', 'n']]


class XcuThread(CmdThread):
    """ Placeholder to a handle xcu command thread. """

    def __init__(self, spsActor, cam, cmdStr, **kwargs):
        self.cam = cam
        actorName = f'xcu_{cam}'
        spsActor.requireModels([actorName])
        CmdThread.__init__(self, spsActor, actorName, cmdStr, **kwargs)

    @property
    def cams(self):
        return [self.cam]


class Slit(EnuThread):
    """ Placeholder to a handle slit command thread. """

    def __init__(self, spsActor, specNum, cmdHead, **kwargs):
        cmdStr = f'slit {cmdHead}'.strip() if 'slit' not in cmdHead else cmdHead
        EnuThread.__init__(self, spsActor, specNum, cmdStr, **kwargs)

    def precheck(self):
        """ Check that the slit in the correct state prior to any movement. """
        state, substate = self.keyVarDict['slitFSM'].getValue(doRaise=False)

        if not (state == 'ONLINE' and substate == 'IDLE'):
            raise ValueError(f'{self.actorName}__slitFSM={state},{substate} != ONLINE,IDLE before moving, aborting ...')


class Iis(EnuThread):
    """ Placeholder to a handle iis command thread. """

    def __init__(self, spsActor, specNum, cmdHead, timeLim=60, **kwargs):
        cmdStr = f'iis {cmdHead}'.strip() if 'iis' not in cmdHead else cmdHead
        EnuThread.__init__(self, spsActor, specNum, cmdStr, timeLim=timeLim, **kwargs)

    def precheck(self):
        """ Check that the slit in the correct state prior to any movement. """
        state, substate = self.keyVarDict['iisFSM'].getValue(doRaise=False)

        if not (state == 'ONLINE' and substate == 'IDLE'):
            raise ValueError(f'{self.actorName}__iisFSM={state},{substate} != ONLINE,IDLE, aborting ...')


class CcdMotors(XcuThread):
    """ Placeholder to a handle CcdMotors command thread. """
    motorIds = dict(a=[1], b=[2], c=[3], piston=[1, 2, 3])

    def __init__(self, spsActor, cam, cmdStr, **kwargs):
        self.motorList = self.motors(a='a' in kwargs, b='b' in kwargs, c='c' in kwargs, piston='piston' in kwargs)
        XcuThread.__init__(self, spsActor, cam, cmdStr, **kwargs)

    def motors(self, **kwargs):
        return list(set(sum([CcdMotors.motorIds[k] for k in kwargs], [])))

    def precheck(self):
        """ Check that the ccdMotors are in the correct state prior to any movement. """
        for i in self.motorList:
            state, _, _, _, _ = self.keyVarDict[f'ccdMotor{i}'].getValue(doRaise=False)
            if state != 'OK':
                raise ValueError(f'{self.actorName}__ccdMotor{i}={state} != OK before moving, aborting ...')
