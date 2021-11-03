import time

import spsActor.utils.cmd as cmdUtils
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from spsActor.utils import exposure
from spsActor.utils.lib import wait, threaded


class LampsControl(QThread):
    """ Placeholder to handle lamp cmd threading. """

    def __init__(self, exp, lampsActor):
        self.exp = exp
        self.lampsActor = lampsActor
        self.cmdVar = None
        self.goSignal = False
        self.aborted = None
        QThread.__init__(self, exp.actor, 'lampsControl')
        QThread.start(self)

    @property
    def isReady(self):
        return self.cmdVar is not None

    def _waitForReadySignal(self, cmd):
        """ Wait for ready signal from lampActor(pfilamps, dcb..).  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='waitForReadySignal', timeLim=180)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    def _go(self, cmd):
        """ Send go command to lampActor. """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='go', timeLim=self.exp.exptime + 60)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    @threaded
    def start(self, cmd):
        """ Full lamp control routine.  """
        try:
            self.cmdVar = self._waitForReadySignal(cmd)
            self.waitForGoSignal()
            self._go(cmd)
            self.exp.finish(cmd)

        except Exception as e:
            self.abort(cmd)
            self.exp.abort(cmd, reason=str(e))

    def waitForGoSignal(self):
        """ Wait for go signal from the shutters.  """
        while not self.goSignal:
            if self.exp.doFinish:
                raise exception.EarlyFinish

            if self.exp.doAbort:
                raise exception.ExposureAborted

            wait()

    def abort(self, cmd):
        """ Send stop command. """
        if self.aborted is None:
            self.aborted = False
            self.actor.safeCall(cmd, actor=self.lampsActor, cmdStr='stop', timeLim=5)
            self.aborted = True

    def declareDone(self, cmd):
        """ Declare exposure is over.  """
        self.actor.safeCall(cmd, actor=self.lampsActor, cmdStr='stop', timeLim=5)

    def finish(self, cmd):
        """ Just a prototype. """
        pass

    def handleTimeout(self):
        """ Just a prototype. """
        pass


class ShutterControlled(LampsControl):
    """ Placeholder to handle lamp cmd threading, in that class exposure time is controlled by shutters. """
    waitBeforeOpening = 2

    @threaded
    def start(self, cmd):
        """ Full lamp control routine.  """
        try:
            self._waitForReadySignal(cmd)
            self.waitForGoSignal()
            self.cmdVar = self._go(cmd)

        except Exception as e:
            self.abort(cmd)
            self.exp.abort(cmd, reason=str(e))

    def _go(self, cmd):
        """ Send go command, no blocking.  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='go noWait', timeLim=10)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        time.sleep(ShutterControlled.waitBeforeOpening)

        return cmdVar


class SpecModuleExposure(exposure.SpecModuleExposure):
    """ Placeholder to handle spectograph module cmd threading. """

    def __init__(self, *args, **kwargs):
        exposure.SpecModuleExposure.__init__(self, *args, **kwargs)
        self.shuttersKeyVar = self.exp.actor.models[self.enu].keyVarDict['shutters']
        self.shuttersKeyVar.addCallback(self.shuttersState)
        self.shuttersOpen = None

    def lightSource(self):
        return self.exp.actor.spsConfig.specModules[self.specName].lightSource

    def integrate(self, cmd):
        """ Command shutters to expose with given overhead. """
        self.exp.waitForReadySignal()
        shutterTime, dateobs = exposure.SpecModuleExposure.integrate(self, cmd,
                                                                     shutterTime=self.exp.exptime + self.exp.shutterOverHead)
        return self.exp.exptime, dateobs

    def shuttersState(self, keyVar):
        """ Shutters state callback, send go signal whenever open. """
        state = keyVar.getValue(doRaise=False)
        self.shuttersOpen = 'open' in state

        if self.shuttersOpen:
            self.actor.bcast.debug(f'text="{self.specName} shutters {state}"')
            self.exp.sendGoLampsSignal()

    def exit(self):
        """ Free up all resources """
        self.shuttersKeyVar.removeCallback(self.shuttersState)
        exposure.SpecModuleExposure.exit(self)


class Exposure(exposure.Exposure):
    """ Lamp controlled exposure time """
    shutterOverHead = 10
    SpecModuleExposureClass = SpecModuleExposure
    LampControlClass = LampsControl

    def __init__(self, *args, **kwargs):
        exposure.Exposure.__init__(self, *args, **kwargs)
        [lightSource] = list(set(th.lightSource() for th in self.smThreads))
        self.lampsThread = self.LampControlClass(self, lampsActor=lightSource.lampsActor)

    @property
    def threads(self):
        return self.smThreads + [self.lampsThread]

    def start(self, cmd, visit):
        """ Full exposure routine. """
        self.lampsThread.start(cmd)
        exposure.Exposure.start(self, cmd, visit=visit)

    def waitForCompletion(self, cmd, visit):
        """ Wait for exposure completion.  """
        fileIds = exposure.Exposure.waitForCompletion(self, cmd, visit=visit)
        self.lampsThread.declareDone(cmd)
        return fileIds

    def waitForReadySignal(self):
        """ Wait ready signal from lampActor. """
        while not self.lampsThread.isReady:
            if self.doFinish:
                raise exception.EarlyFinish

            if self.doAbort:
                raise exception.ExposureAborted

            wait()

        self.actor.bcast.debug(f'text="{self.lampsThread.lampsActor} is ready !!!"')

    def sendGoLampsSignal(self):
        """ Wait for all shutters to be opened to send go signal. """
        if all([thread.shuttersOpen for thread in self.smThreads]):
            self.lampsThread.goSignal = True


class ShutterExposure(Exposure):
    """ Placeholder to handle spectograph module cmd threading. """
    shutterOverHead = 0
    LampControlClass = ShutterControlled

    def waitForReadySignal(self):
        self.sendGoLampsSignal()
        return Exposure.waitForReadySignal(self)

    def sendGoLampsSignal(self):
        if all(sum([thread.currently('integrating') for thread in self.smThreads], [])):
            self.lampsThread.goSignal = True
