from ics.utils.threading import threaded
from spsActor.utils.lampsControl import LampsControl


class IISControl(LampsControl):
    """ Placeholder to handle IIS cmd threading. """
    # IIS command syntax is a bit different.
    goCmd = 'iis go'
    abortCmd = 'iis abort'

    def __init__(self, exp, enuName):
        LampsControl.__init__(self, exp, enuName, threadName=f'iisControl_{enuName}')

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