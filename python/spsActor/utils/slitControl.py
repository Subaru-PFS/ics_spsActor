import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.threading import threaded


class SlitControl(QThread):
    """ Placeholder to handle slit cmd threading. """
    timeMargin = 20
    abortTimeLim = 15
    abortCmd = 'slit abort'

    def __init__(self, exp, enuName):
        self.exp = exp
        self.enuName = enuName
        (self.pixelMin, self.pixelMax) = exp.pixelRange

        self.cmdVar = None
        self.aborted = None
        self.goSignal = False

        QThread.__init__(self, exp.actor, 'slitControl')
        QThread.start(self)

    def _go(self, cmd):
        """ Send linearVerticalMove command to enuActor. """
        cmdVar = self.actor.crudeCall(cmd, actor=self.enuName,
                                      cmdStr=f'slit linearVerticalMove expTime={self.exp.exptime} '
                                             f'pixelRange={self.pixelMin},{self.pixelMax}',
                                      timeLim=self.exp.exptime + SlitControl.timeMargin)

        if cmdVar.didFail:
            raise exception.SlitMoveFailed(self.enuName, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    @threaded
    def start(self, cmd):
        """ Full lamp control routine.  """
        try:
            self._waitForGoSignal()
            # Go for linear motion.
            self._go(cmd)
        except Exception as e:
            self.abort(cmd)
            self.exp.abort(cmd, reason=str(e))

    def _waitForGoSignal(self):
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
            self.actor.safeCall(cmd, actor=self.enuName, cmdStr=self.abortCmd, timeLim=SlitControl.abortTimeLim)
            self.aborted = True

    def declareDone(self, cmd):
        """ Declare exposure is over.  """
        pass

    def finish(self, cmd):
        """ Just a prototype. """
        pass

    def handleTimeout(self):
        """ Just a prototype. """
        pass
