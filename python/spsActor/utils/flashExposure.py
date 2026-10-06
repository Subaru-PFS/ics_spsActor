import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from ics.utils.threading import threaded
from spsActor.utils import lampsControl, lampsExposure


class SpecModuleFlash(lampsExposure.SpecModuleExposure):
    """Shutters of one spectrograph module, opened for a lamp pulse with no detector involved."""

    def __init__(self, *args, **kwargs):
        self.done = False
        lampsExposure.SpecModuleExposure.__init__(self, *args, **kwargs)

    @property
    def isFinished(self):
        return self.done

    def camExposures(self, cams):
        """No detector is driven, the cameras only select the shutters."""
        return []

    @threaded
    def expose(self, cmd, visit):
        """Open the shutters once the lamps are ready, the pulse closes them."""
        try:
            self.integrate(cmd)
        except Exception as e:
            self.exp.abort(cmd, reason=str(e))
        finally:
            self.done = True


class Flash(lampsExposure.Exposure):
    """Lamp pulse through open shutters, from pfilamps or iis, with no visit and no detector.

    The lamps must have been prepared beforehand, as for a lamp-timed exposure.
    """
    SpecModuleExposureClass = SpecModuleFlash

    def __init__(self, actor, exptime, cams, doLamps=False, doIIS=False, **kwargs):
        lampsExposure.Exposure.__init__(self, actor, None, exptype='flash', exptime=exptime, cams=cams,
                                        doLamps=doLamps, doIIS=doIIS, **kwargs)

    def waitForReadySignal(self):
        """Wait until every lamp of the flash, iis included, is warmed up."""
        while not all(thread.isReady for thread in self.lampsThreads):
            if self.doFinish:
                raise exception.EarlyFinish

            if self.doAbort:
                raise exception.ExposureAborted

            pfsTime.sleep.millisec()

    def waitForCompletion(self, cmd, visit=None):
        """Run the flash until the shutters are closed again."""
        self.start(cmd, visit)

        while not self.isFinished:
            pfsTime.sleep.millisec()

        if not self.failures and not any(thread.wentGo for thread in self.definingLampsThreads):
            self.failures.add('lamps never fired')

        # a failed flash ends the run, so whatever was going to use the light next is not going to happen.
        if self.failures:
            self.isLast = True
            self.releaseLamps(cmd)

    def releaseLamps(self, cmd):
        """Stop every lamp actor of the flash, fired or only warmed up, and the backgrounded ones."""
        lampsActors = [thread.lampsActor for thread in self.lampsThreads
                       if isinstance(thread, lampsControl.LampsControl)] + self.bckIlluminators

        with self.illuminatorLock:
            toStop = [actor for actor in dict.fromkeys(lampsActors) if actor not in self.stoppedIlluminators]
            self.stoppedIlluminators.update(toStop)

        for lampsActor in toStop:
            self.sendStop(cmd, lampsActor)

    def genIlluminationStatus(self):
        """fiberIllumination describes the frames of a visit, a flash has neither."""
        pass
