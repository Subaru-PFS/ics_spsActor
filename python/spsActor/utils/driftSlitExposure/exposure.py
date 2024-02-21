import ics.utils.time as pfsTime
from ics.utils.threading import threaded
from spsActor.utils import exposure, slitControl
import spsActor.utils.exception as exception

class SpecModuleExposure(exposure.SpecModuleExposure):
    """ Placeholder to handle spectograph module cmd threading. """

    def __init__(self, *args, **kwargs):
        exposure.SpecModuleExposure.__init__(self, *args, **kwargs)
        self.slitStateKeyVar = self.exp.actor.models[self.enuName].keyVarDict['slitAtSpeed']
        self.slitStateKeyVar.addCallback(self.slitState)
        self.slitSliding = False

        self.slitControl = slitControl.SlitControl(self.exp, self.enuName)

    @threaded
    def expose(self, cmd, visit):
        """Full exposure routine, exceptions are catched and handled under the cover."""

        try:
            self.wipe(cmd)
            # just interleave slit motion with normal expose routine.
            self.startSlitMotionAndWait()
            exptime, dateobs = self.integrate(cmd)
            self.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        except Exception as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    def startSlitMotionAndWait(self):
        """Send go signal and wait for slit to be at speed."""
        # sending go signal.
        self.slitControl.goSignal = True

        while not self.slitSliding:
            if self.exp.doAbort:
                raise exception.ExposureAborted

            pfsTime.sleep.millisec()

    def slitState(self, keyVar):
        """Slit state callback, call shuttersOpenCB() whenever open."""
        atSpeed = bool(keyVar.getValue(doRaise=False))

        # track slit state.
        self.actor.bcast.debug(f'text="{self.specName} slitAtSpeed={atSpeed}"')
        self.slitSliding = atSpeed

    def exit(self):
        """Free up all resources."""
        # remove slit callback.
        self.slitStateKeyVar.removeCallback(self.slitState)
        exposure.SpecModuleExposure.exit(self)


class Exposure(exposure.Exposure):
    SpecModuleExposureClass = SpecModuleExposure

    def __init__(self, *args, slideSlitPixelRange, **kwargs):
        self.pixelRange = slideSlitPixelRange

        exposure.Exposure.__init__(self, *args, **kwargs)

    @property
    def slitThreads(self):
        return list(filter(None, [smThread.slitControl for smThread in self.smThreads]))

    @property
    def threads(self):
        return self.smThreads + self.lampsThreads + self.slitThreads

    def start(self, cmd, visit):
        """Start all spectrograph module exposures."""
        exposure.Exposure.start(self, cmd, visit)

        # start slit thread.
        for thread in self.slitThreads:
            thread.start(cmd)
