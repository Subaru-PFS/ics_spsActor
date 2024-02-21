import time

import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.sps.lamps.utils.lampState import allLamps
from ics.utils.threading import threaded


class LampsControl(QThread):
    """ Placeholder to handle lamp cmd threading. """
    goCmd = 'go'
    abortCmd = 'stop'
    waitForReadySignalTimeLim = 300
    goTimeMargin = 60
    abortTimeLim = stopTimeLim = goNoWaitTimeLim = 10

    def __init__(self, exp, lampsActor, threadName='lampsControl'):

        self.exp = exp
        self.lampsActor = lampsActor
        self.cmdVar = None
        self.goSignal = False
        self.aborted = None
        QThread.__init__(self, exp.actor, threadName)
        QThread.start(self)

    @property
    def isReady(self):
        return self.cmdVar is not None

    @staticmethod
    def lampState(keyVar, shutterOpenTime, shutterCloseTime):
        # retrieve keyword value.
        state, offTime, onTime = keyVar.getValue()
        # converting iso-formatted date to timestamp.
        offTime = pfsTime.Time.fromisoformat(offTime).timestamp()
        onTime = pfsTime.Time.fromisoformat(onTime).timestamp()
        shutterOpenTime = pfsTime.Time.fromisoformat(shutterOpenTime).timestamp()
        shutterCloseTime = pfsTime.Time.fromisoformat(shutterCloseTime).timestamp()

        # applying crude logic see INSTRM-1052 .
        offTime = shutterCloseTime if offTime < onTime else offTime
        start = min(max(onTime, shutterOpenTime), shutterCloseTime)
        end = max(min(offTime, shutterCloseTime), shutterOpenTime)
        lampExptime = end - start

        return lampExptime > 0

    @staticmethod
    def checkIllumination(visit, enuKeyVarDict, lampKeyVarDict):
        """"""
        lastVisit, startedAt, openAt, endedAt, closedAt = enuKeyVarDict['shutterTimings'].getValue()

        if lastVisit != visit:
            return False

        illuminated = []
        # checking all possible lamps.
        for lamp in allLamps:
            try:
                illuminated.append(LampsControl.lampState(lampKeyVarDict[lamp], openAt, closedAt))
            except (KeyError, ValueError):
                continue

        return any(illuminated)

    def _waitForReadySignal(self, cmd):
        """ Wait for ready signal from lampActor(pfilamps, dcb..).  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='waitForReadySignal',
                                      timeLim=LampsControl.waitForReadySignalTimeLim)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    def _go(self, cmd):
        """ Send go command to lampActor. """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr=self.goCmd,
                                      timeLim=self.exp.exptime + LampsControl.goTimeMargin)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    @threaded
    def start(self, cmd):
        """ Full lamp control routine.  """
        try:
            self.cmdVar = self._waitForReadySignal(cmd)
            # Wait for the go signal, namely when all shutters are opened.
            self.waitForGoSignal()
            # Ask lamp controller to pulse lamps with the configured timing.
            self._go(cmd)
            # Lamp(s) have been pulsed, exposure can now finish immediately.
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

            pfsTime.sleep.millisec()

    def abort(self, cmd):
        """ Send stop command. """
        if self.aborted is None:
            self.aborted = False
            # self.actor.safeCall(cmd, actor=self.lampsActor, cmdStr=self.abortCmd, timeLim=LampsControl.abortTimeLim)
            self.aborted = True

    def declareDone(self, cmd):
        """ Declare exposure is over.  """
        pass
        # self.actor.safeCall(cmd, actor=self.lampsActor, cmdStr='stop', timeLim=LampsControl.stopTimeLim)

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
            # Still wait for a go signal from the last shutter thread, namely when detectors are all ready.
            self.waitForGoSignal()
            # When _go() returns, here without blocking, it declares self.isReady=True, thus the shutters can be opened.
            self.cmdVar = self._go(cmd)

        except Exception as e:
            self.abort(cmd)
            self.exp.abort(cmd, reason=str(e))

    def _go(self, cmd):
        """ Send go command, no blocking.  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='go noWait',
                                      timeLim=LampsControl.goNoWaitTimeLim)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        # Wait for some additional time before opening shutters to be fully safe.
        time.sleep(ShutterControlled.waitBeforeOpening)

        return cmdVar


class NoLamps(QThread):
    def __init__(self, exp, threadName='noLampsControl'):
        self.exp = exp
        self.isReady = True
        self.lampsActor = 'noLamps'

        QThread.__init__(self, exp.actor, threadName)
        QThread.start(self)

    @threaded
    def start(self, cmd):
        """ Full lamp control routine.  """
        pass

    def waitForGoSignal(self):
        """ Wait for go signal from the shutters.  """
        pass

    def abort(self, cmd):
        """ Send stop command. """
        pass

    def declareDone(self, cmd):
        """ Declare exposure is over.  """
        pass

    def finish(self, cmd):
        """ Just a prototype. """
        pass

    def handleTimeout(self):
        """ Just a prototype. """
        pass
