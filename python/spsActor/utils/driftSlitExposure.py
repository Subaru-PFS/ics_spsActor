from spsActor.utils import exposure, lampsExposure, slitControl


class SpecModuleExposure(lampsExposure.SpecModuleExposure):
    """ Placeholder to handle spectograph module cmd threading. """

    def __init__(self, *args, **kwargs):
        lampsExposure.SpecModuleExposure.__init__(self, *args, **kwargs)
        self.slitStateKeyVar = self.exp.actor.models[self.enuName].keyVarDict['slitAtSpeed']
        self.slitStateKeyVar.addCallback(self.slitState)
        self.slitSliding = False

        self.slitControl = slitControl.SlitControl(self.exp, self.enuName)

    def slitState(self, keyVar):
        """Slit state callback, call shuttersOpenCB() whenever open."""
        atSpeed = bool(keyVar.getValue(doRaise=False))

        # track slit state.
        self.actor.bcast.debug(f'text="{self.specName} slitAtSpeed={atSpeed}"')

        self.slitSliding = atSpeed

        if self.slitSliding:
            self.slitSlidingCB()

    def slitSlidingCB(self):
        """ Shutters state callback, send go signal whenever open. """
        # send go signal.
        self.exp.sendGoLampsSignal()

    def shuttersOpenCB(self):
        """ Shutters state callback, send go signal whenever open. """
        exposure.SpecModuleExposure.shuttersOpenCB(self)

        # send go slit signal.
        if not self.slitSliding:
            self.exp.sendGoSlitSignal()

    def exit(self):
        """Free up all resources."""
        # remove slit callback.
        self.slitStateKeyVar.removeCallback(self.slitState)
        lampsExposure.SpecModuleExposure.exit(self)


class Exposure(lampsExposure.Exposure):
    SpecModuleExposureClass = SpecModuleExposure
    expTimeOverHead = 5
    shutterOverHead = 15

    def __init__(self, *args, slideSlitPixelRange, **kwargs):
        self.pixelRange = slideSlitPixelRange
        lampsExposure.Exposure.__init__(self, *args, **kwargs)

    @property
    def slitThreads(self):
        return list(filter(None, [smThread.slitControl for smThread in self.smThreads]))

    @property
    def threads(self):
        return self.smThreads + self.lampsThreads + self.slitThreads

    def start(self, cmd, visit):
        """Start all spectrograph module exposures."""
        lampsExposure.Exposure.start(self, cmd, visit)

        # start slit thread.
        for thread in self.slitThreads:
            thread.start(cmd)

    def sendGoSlitSignal(self):
        """ Wait for all shutters to be opened to send go signal. """
        if all([thread.shuttersOpen for thread in self.smThreads]):
            self.sendAllGoSlitSignal()

    def sendAllGoSlitSignal(self):
        """"""
        for thread in self.smThreads:
            thread.slitControl.goSignal = True

    def sendGoLampsSignal(self):
        """ Wait for all shutters to be opened to send go signal. """
        if all([thread.slitSliding for thread in self.smThreads]):
            self.lampsThread.goSignal = True
