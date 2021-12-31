import time

import ics.utils.cmd as cmdUtils
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.threading import threaded
from spsActor.utils.lib import wait


class LampsControl(QThread):
    """ Placeholder to handle lamp cmd threading. """
    goCmd = 'go'
    abortCmd = 'stop'

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

    def _waitForReadySignal(self, cmd):
        """ Wait for ready signal from lampActor(pfilamps, dcb..).  """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='waitForReadySignal', timeLim=180)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    def _go(self, cmd):
        """ Send go command to lampActor. """
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr=self.goCmd, timeLim=self.exp.exptime + 60)

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

            wait()

    def abort(self, cmd):
        """ Send stop command. """
        if self.aborted is None:
            self.aborted = False
            self.actor.safeCall(cmd, actor=self.lampsActor, cmdStr=self.abortCmd, timeLim=5)
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


class IISControl(LampsControl):
    """ Placeholder to handle IIS cmd threading. """
    # IIS command syntax is a bit different.
    goCmd = 'iis go'
    abortCmd = 'iis abort'

    def __init__(self, exp, enuActor):
        LampsControl.__init__(self, exp, enuActor, threadName=f'iisControl_{enuActor}')

    @threaded
    def start(self, cmd):
        """ Full lamp control routine.  """
        try:
            # dont wait for ready signal, at least for now.
            # self.cmdVar = self._waitForReadySignal(cmd)
            # Wait for the go signal, namely when all shutters are opened.
            self.waitForGoSignal()
            # Ask lamp controller to pulse lamps with the configured timing.
            self._go(cmd)

        except Exception as e:
            self.abort(cmd)
            self.exp.abort(cmd, reason=str(e))


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
        cmdVar = self.actor.crudeCall(cmd, actor=self.lampsActor, cmdStr='go noWait', timeLim=10)

        if cmdVar.didFail:
            raise exception.LampsFailed(self.lampsActor, cmdUtils.interpretFailure(cmdVar))

        # Wait for some additional time before opening shutters to be fully safe.
        time.sleep(ShutterControlled.waitBeforeOpening)

        return cmdVar
