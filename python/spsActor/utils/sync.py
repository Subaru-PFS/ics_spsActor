import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.threading import threaded


class SpsCmd(object):
    """ Placeholder to synchronise multiple command thread. """

    def __init__(self, spsActor):
        self.spsActor = spsActor
        self.cmdThd = None

        self.didFail = False
        self.failures = exception.Failures()

    @property
    def finished(self):
        return all([th.finished for th in self.cmdThd])

    def attachThreads(self, threads):
        """ Attach command threads. """
        self.cmdThd = threads

    def process(self, cmd):
        """ Call, synchronise and handle results. """
        self.inform(cmd)
        self.call(cmd)
        didFail = self.sync()

        if didFail:
            cmd.fail(f'text="{self.failures.format()}"')
        else:
            self.finish(cmd)

        self.clear()

    def call(self, cmd):
        """ Call for each command thread. """
        for th in self.cmdThd:
            th.call(cmd)

    def sync(self):
        """ Wait for command thread to be finished. """
        while not self.finished:
            pfsTime.sleep.millisec()

        return self.didFail

    def inform(self, cmd):
        """ Prototype. """
        pass

    def fail(self, reason):
        """ Called from command threads, something wrong happened that's the reason. """
        self.didFail = True
        self.failures.add(reason)

    def finish(self, cmd):
        """ Prototype. """
        cmd.finish()

    def clear(self):
        """ Kill all command threads before exiting. """
        for th in self.cmdThd:
            th.exit()

        self.cmdThd.clear()


class CmdThread(QThread):
    """ Placeholder to handle single cmd threading """

    def __init__(self, spsCmd, **cmdCall):
        self.spsCmd = spsCmd
        self.cmdCall = cmdCall

        self.cmdVar = None
        self.cancelled = False

        sw, identifier = cmdCall['actor'].split('_')
        QThread.__init__(self, spsCmd.spsActor, identifier)
        QThread.start(self)

    @property
    def finished(self):
        return self.cancelled or self.cmdVar is not None

    @property
    def fullCmdStr(self):
        return f'{self.cmdCall["actor"]} {self.cmdCall["cmdStr"]}'

    def _call(self, cmd):
        """ Call the command and handle the reply. """
        cmdVar = self.actor.crudeCall(cmd, **self.cmdCall)

        if cmdVar.didFail:
            raise exception.factory(type(self).__name__, self.name, cmdUtils.interpretFailure(cmdVar))

        return cmdVar

    @threaded
    def call(self, cmd):
        """ Call the command modulo pre-post check."""
        try:
            self.precheck(cmd)
            cmdVar = self._call(cmd)
            self.postcheck(cmd)
            self.cmdVar = cmdVar

        except Exception as e:
            self.cancelled = True
            self.spsCmd.fail(reason=str(e))

    def precheck(self, cmd):
        """ To be called before the actual command. """
        cmd.inform(f'text="calling {self.fullCmdStr} timeLim({self.cmdCall["timeLim"]})"')

    def postcheck(self, cmd):
        """ To be called after the actual command. """
        cmd.inform(f'text="{self.fullCmdStr} succeed !"')

    def handleTimeout(self):
        """ Just a prototype. """
        pass
