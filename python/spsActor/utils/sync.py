import time
from functools import partial

from actorcore.QThread import QThread


def putMsg(func):
    def wrapper(self, cmd, *args, **kwargs):
        self.putMsg(partial(func, self, cmd, *args, **kwargs))

    return wrapper


def threaded(func):
    @putMsg
    def wrapper(self, cmd, *args, **kwargs):
        try:
            return func(self, cmd, *args, **kwargs)
        except Exception as e:
            cmd.fail('text=%s' % self.actor.strTraceback(e))

    return wrapper


class SyncCmd(object):
    def __init__(self, actor, cmdList):
        self.cmdThd = [CmdThread(actor, cmdStr=cmdStr) for cmdStr in cmdList]

    def process(self, cmd):
        self.call(cmd)
        self.sync()
        self.exit()

    def call(self, cmd):
        for th in self.cmdThd:
            th.call(cmd)

    def sync(self):
        while None in [th.cmdVar for th in self.cmdThd]:
            time.sleep(1)

    def exit(self):
        for ti in self.cmdThd:
            ti.exit()


class CmdThread(QThread):
    def __init__(self, actor, cmdStr):
        self.cmdVar = None
        self.actorName, self.cmdStr = cmdStr.split(' ', 1)
        QThread.__init__(self, actor, str(time.time()))
        QThread.start(self)

    @threaded
    def call(self, cmd):
        cmd.inform(f'text="calling {self.actorName} {self.cmdStr}"')
        cmdVar = self.actor.safeCall(actor=self.actorName, cmdStr=self.cmdStr, forUserCmd=cmd)

        if not cmdVar.didFail:
            cmd.inform(f'text="{self.actorName} {self.cmdStr} OK"')

        self.cmdVar = cmdVar
