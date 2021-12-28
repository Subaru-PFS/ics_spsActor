import spsActor.utils.exception as exception
from spsActor.utils import exposure, lampsControl
from spsActor.utils.lib import wait


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
        # Block until the lampThread gives its ready signal.
        self.exp.waitForReadySignal()
        # shutter time is the exptime + some overhead, the exposure will be finished asap in anycase.
        shutterTime = self.exp.exptime + self.exp.shutterOverHead
        # Send proceed with regular shutter integration, the callback will asynchronously give the go to the lamps.
        shutterTime, dateobs = exposure.SpecModuleExposure.integrate(self, cmd, shutterTime=shutterTime)
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
    LampControlClass = lampsControl.LampsControl

    def __init__(self, *args, **kwargs):
        exposure.Exposure.__init__(self, *args, **kwargs)
        [lightSource] = list(set(th.lightSource() for th in self.smThreads))
        self.lampsThread = self.LampControlClass(self, lampsActor=lightSource.lampsActor)

    @property
    def threads(self):
        return self.smThreads + [self.lampsThread]

    def start(self, cmd, visit):
        """ Full exposure routine. """
        # Start lamp thread, ready signal will be raised later.
        self.lampsThread.start(cmd)
        # Normal exposure, except that shutter thread(s) is/are blocked until ready signal.
        # Lamp thread is then waiting on go signal which only happen when all shutters are all opened.
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
    LampControlClass = lampsControl.ShutterControlled

    def waitForReadySignal(self):
        """ is called by the shutters, that gives the signal to open the shutters."""
        self.sendGoLampsSignal()
        # In this case, lampThread.isReady==True when lampThread._go() returns, eg when lamps are actually turned on.
        lampsAreTurnedOn = Exposure.waitForReadySignal(self)
        # Lamps are turned on, shutters can be opened.
        return lampsAreTurnedOn

    def sendGoLampsSignal(self):
        """ Send go lamp signal only when all shutter threads are ready."""
        # Basically means that all detectors are ready to receive photons.
        if all(sum([thread.currently('integrating') for thread in self.smThreads], [])):
            self.lampsThread.goSignal = True
