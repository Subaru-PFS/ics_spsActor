import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from spsActor.utils import exposure
from spsActor.utils.lib import wait, threaded, interpretFailure


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
        """ Create underlying SmExposure threads.  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='waitForReadySignal', timeLim=180)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, interpretFailure(cmdVar))

        return cmdVar

    def _go(self, cmd):
        """ Create underlying SmExposure threads.  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='go', timeLim=180)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, interpretFailure(cmdVar))

        return cmdVar

    @threaded
    def start(self, cmd):
        """ Create underlying SmExposure threads.  """
        try:
            self.cmdVar = self._waitForReadySignal(cmd)
            self.waitForGoSignal()
            self._go(cmd)
            self.exp.finish(cmd)

        except Exception as e:
            self.abort(cmd)
            self.exp.abort(cmd, reason=str(e))

    def waitForGoSignal(self):
        """ Create underlying SmExposure threads.  """
        while not self.goSignal:
            if self.exp.doFinish:
                raise exception.EarlyFinish

            if self.exp.doAbort:
                raise exception.ExposureAborted

            wait()

    def abort(self, cmd):
        """ Create underlying SmExposure threads.  """
        if self.aborted is None:
            self.aborted = False
            self.actor.safeCall(cmd, actor=self.lampsActor, cmdStr='stop', timeLim=5)
            self.aborted = True

    def finish(self, cmd):
        """ Just a prototype. """
        pass

    def handleTimeout(self):
        """ Just a prototype. """
        pass


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
        """ Integrate for both calib and regular exposure """
        self.exp.waitForReadySignal()
        shutterTime, dateobs = exposure.SpecModuleExposure.integrate(self, cmd, shutterTime=self.exp.exptime + 5)
        return self.exp.exptime, dateobs

    def shuttersState(self, keyVar):
        """ Clear all running CcdExposure. """
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
    SpecModuleExposureClass = SpecModuleExposure

    def __init__(self, *args, **kwargs):
        exposure.Exposure.__init__(self, *args, **kwargs)
        [lightSource] = list(set(th.lightSource() for th in self.smThreads))
        self.lampsThread = LampsControl(self, lampsActor=lightSource.lampsActor)

    @property
    def threads(self):
        return self.smThreads + [self.lampsThread]

    def start(self, cmd, visit):
        """ Start all spectrograph module exposures. """
        self.lampsThread.start(cmd)
        exposure.Exposure.start(self, cmd, visit=visit)

    def waitForCompletion(self, cmd, visit):
        """ Create underlying specModuleExposure threads.  """
        fileIds = exposure.Exposure.waitForCompletion(self, cmd, visit=visit)
        self.lampsThread.abort(cmd)
        return fileIds

    def waitForReadySignal(self):
        """ Free up all resources """
        while not self.lampsThread.isReady:
            if self.doFinish:
                raise exception.EarlyFinish

            if self.doAbort:
                raise exception.ExposureAborted

            wait()

        self.actor.bcast.debug(f'text="{self.lampsThread.lampsActor} is ready !!!"')

    def sendGoLampsSignal(self):
        """ Start all spectrograph module exposures. """
        if all([thread.shuttersOpen for thread in self.smThreads]):
            self.lampsThread.goSignal = True
