import time

from actorcore.QThread import QThread
from spsActor.utils import wait, threaded


class Sync(object):
    """ Placeholder to synchronise multiple command thread. """

    def __init__(self, cmdThd):
        self.cmdThd = cmdThd

    def __del__(self):
        self.exit()

    @property
    def finished(self):
        return all([th.finished for th in self.cmdThd])

    def process(self, cmd):
        """ Call, synchronise and handle results. """
        self.inform(cmd)
        self.call(cmd)
        self.sync()
        ret = self.examinate()
        self.finish(cmd)
        return ret

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

    def inform(self, cmd):
        """ Prototype. """
        pass

    def finish(self, cmd):
        """ Prototype. """
        pass

    def exit(self):
        """ Kill all command threads before exiting. """
        for th in self.cmdThd:
            th.exit()


class RdaMove(Sync):
    def __init__(self, spsActor, specNums, targetPosition):
        cmdThd = [RdaThread(spsActor, specNum, targetPosition) for specNum in specNums]
        Sync.__init__(self, cmdThd)
        self.targetPosition = targetPosition

    def inform(self, cmd):
        specNames = ','.join([th.specName for th in self.cmdThd])
        cmd.inform(f'text="rda moving to {self.targetPosition} position for {specNames}"')

    def finish(self, cmd):
        specNames = ','.join([th.specName for th in self.cmdThd])
        cmd.finish(f'text="rdaMove({specNames}) to {self.targetPosition} position completed"')

    def examinate(self):
        failures = [th for th in self.cmdThd if th.failed]
        if failures:
            raise RuntimeError(f'RdaMove failed for {",".join([failure.specName for failure in failures])} !!!')


class BiaSwitch(Sync):
    def __init__(self, spsActor, specNums, state, **kwargs):
        cmdThd = [BiaThread(spsActor, specNum, state, **kwargs) for specNum in specNums]
        Sync.__init__(self, cmdThd)
        self.state = state

    def inform(self, cmd):
        specNames = ','.join([th.specName for th in self.cmdThd])
        cmd.inform(f'text="switching bia {self.state} for {specNames}"')

    def finish(self, cmd):
        specNames = ','.join([th.specName for th in self.cmdThd])
        cmd.finish(f'text="bia switched {self.state} for {specNames}"')

    def examinate(self):
        failures = [th for th in self.cmdThd if th.failed]
        if failures:
            raise RuntimeError(f'RdaMove failed for {",".join([failure.specName for failure in failures])} !!!')


class SlitMove(Sync):
    def __init__(self, spsActor, specNums, cmdHead, **kwargs):
        cmdThd = [SlitThread(spsActor, specNum, cmdHead, **kwargs) for specNum in specNums]
        Sync.__init__(self, cmdThd)


class CcdMotorsMove(Sync):
    def __init__(self, spsActor, cams, cmdHead, **kwargs):
        cmdStr = f'motors {cmdHead}'.strip() if 'motors' not in cmdHead else cmdHead
        cmdThd = [CcdMotorsThread(spsActor, cam, cmdStr, **kwargs) for cam in cams]
        Sync.__init__(self, cmdThd)


class IisSwitch(Sync):
    def __init__(self, spsActor, specNums, cmdHead, **kwargs):
        cmdThd = [IisThread(spsActor, specNum, cmdHead, **kwargs) for specNum in specNums]
        Sync.__init__(self, cmdThd)


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
            self.precheck(cmd)
            cmdVar = self.actor.safeCall(cmd, actor=self.actorName, cmdStr=self.cmdStr, timeLim=self.timeLim,
                                         **self.kwargs)
            self.postcheck(cmd)
            self.cmdVar = cmdVar
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))
            self.cancel()

    def precheck(self, cmd):
        """ Condition(s) to be checked before firing the command. """
        pass

    def postcheck(self, cmd):
        """ Condition(s) to be checked before firing the command. """
        pass

    def cancel(self):
        """ Cancel the command. """
        self.cancelled = True


class EnuThread(CmdThread):
    """ Placeholder to a handle enu command thread. """
    controller = ''

    def __init__(self, spsActor, specNum, cmdStr, timeLim=60, **kwargs):
        self.specNum = specNum
        actorName = f'enu_{self.specName}'
        spsActor.requireModels([actorName])
        CmdThread.__init__(self, spsActor, actorName, cmdStr, timeLim=timeLim, **kwargs)

    @property
    def specName(self):
        return f'sm{self.specNum}'

    @property
    def cams(self):
        return [f'{arm}{self.specNum}' for arm in ['b', 'r', 'n']]

    def genKeys(self, cmd):
        """ Check that the rda in the correct state prior to any movement. """
        pass

    def precheck(self, cmd):
        """ Check that the rda in the correct state prior to any movement. """
        FSM = f'{self.controller}FSM'
        state, substate = self.keyVarDict[FSM].getValue(doRaise=False)

        if not (state == 'ONLINE' and substate == 'IDLE'):
            raise ValueError(f'{self.actorName}__{FSM}={state},{substate} != ONLINE,IDLE before starting ...')

        self.genKeys(cmd)

    def postcheck(self, cmd):
        """ Check that the rda in the correct state prior to any movement. """
        self.genKeys(cmd)


class SlitThread(EnuThread):
    controller = 'slit'
    """ Placeholder to a handle slit command thread. """

    def __init__(self, spsActor, specNum, cmdHead, **kwargs):
        cmdStr = f'slit {cmdHead}'.strip() if 'slit' not in cmdHead else cmdHead
        EnuThread.__init__(self, spsActor, specNum, cmdStr, **kwargs)

    def genKeys(self, cmd):
        focus, ditherY, ditherX, __, __, __ = self.keyVarDict['slit'].getValue(doRaise=False)

        cmd.inform(f'{self.specName}slitFocus={focus}')
        cmd.inform(f'{self.specName}slitDitherX={ditherX}')
        cmd.inform(f'{self.specName}slitDitherY={ditherY}')


class RdaThread(EnuThread):
    """ Placeholder to a handle Rda command thread. """
    controller = 'rexm'

    def __init__(self, spsActor, specNum, targetPosition):
        cmdStr = f'rexm moveTo {targetPosition}'
        EnuThread.__init__(self, spsActor, specNum, cmdStr, timeLim=180)

    def genKeys(self, cmd):
        position = self.keyVarDict['rexm'].getValue(doRaise=False)
        cmd.inform(f'{self.specName}rda={position}')


class BiaThread(EnuThread):
    """ Placeholder to a handle Rda command thread. """
    controller = 'biasha'

    def __init__(self, spsActor, specNum, state, **kwargs):
        cmdStr = f'bia {state}'
        EnuThread.__init__(self, spsActor, specNum, cmdStr, **kwargs)

    def precheck(self, cmd):
        """ Check that the rda in the correct state prior to any movement. """
        FSM = f'{self.controller}FSM'
        state, substate = self.keyVarDict[FSM].getValue(doRaise=False)
        print()

        if not (state == 'ONLINE' and substate in ['IDLE', 'BIA']):
            raise ValueError(f'{self.actorName}__{FSM}={state},{substate} dont match BIA operation...')

        self.genKeys(cmd)

    def genKeys(self, cmd):
        state = self.keyVarDict['bia'].getValue(doRaise=False)
        cmd.inform(f'{self.specName}bia={state}')


class IisThread(EnuThread):
    """ Placeholder to a handle iis command thread. """
    controller = 'iis'

    def __init__(self, spsActor, specNum, cmdHead, timeLim=60, **kwargs):
        cmdStr = f'iis {cmdHead}'.strip() if 'iis' not in cmdHead else cmdHead
        EnuThread.__init__(self, spsActor, specNum, cmdStr, timeLim=timeLim, **kwargs)


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


class CcdMotorsThread(XcuThread):
    """ Placeholder to a handle CcdMotors command thread. """
    motorIds = dict(a=[1], b=[2], c=[3], piston=[1, 2, 3])

    def __init__(self, spsActor, cam, cmdStr, **kwargs):
        self.motorList = self.motors(a='a' in kwargs, b='b' in kwargs, c='c' in kwargs, piston='piston' in kwargs)
        XcuThread.__init__(self, spsActor, cam, cmdStr, **kwargs)

    def motors(self, **kwargs):
        return list(set(sum([CcdMotorsThread.motorIds[k] for k in kwargs], [])))

    def precheck(self, cmd):
        """ Check that the ccdMotors are in the correct state prior to any movement. """
        for i in self.motorList:
            state, _, _, _, _ = self.keyVarDict[f'ccdMotor{i}'].getValue(doRaise=False)
            if state != 'OK':
                raise ValueError(f'{self.actorName}__ccdMotor{i}={state} != OK before moving, aborting ...')
