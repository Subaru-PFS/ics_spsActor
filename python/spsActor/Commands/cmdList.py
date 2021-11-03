import spsActor.utils.cmd as cmdUtils
import spsActor.utils.sync as sync


class RdaMove(sync.SpsCmd):
    timeLim = 180

    def __init__(self, spsActor, specNums, targetPosition):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = f'rexm moveTo {targetPosition}'
        self.attachThreads([RdaMoveCmd(self, specNum, cmdStr, RdaMove.timeLim) for specNum in specNums])


class SlitMove(sync.SpsCmd):
    timeLim = 30

    def __init__(self, spsActor, specNums, cmdHead='', **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'slit {cmdHead}', **kwargs)
        self.attachThreads([SlitMoveCmd(self, specNum, cmdStr, SlitMove.timeLim) for specNum in specNums])


class BiaSwitch(sync.SpsCmd):
    timeLim = 10

    def __init__(self, spsActor, specNums, state, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'bia {state}', **kwargs)
        self.attachThreads([BiaCmd(self, specNum, cmdStr, BiaSwitch.timeLim) for specNum in specNums])


class CcdMotorsMove(sync.SpsCmd):
    timeLim = 30

    def __init__(self, spsActor, cams, cmdHead, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'motors {cmdHead}', **kwargs)
        self.attachThreads([CcdMotorsMoveCmd(self, cam, cmdStr, CcdMotorsMove.timeLim) for cam in cams])


class RdaMoveCmd(sync.CmdThread):
    def __init__(self, spsCmd, specNum, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'enu_sm{specNum}', cmdStr=cmdStr, timeLim=timeLim)


class SlitMoveCmd(sync.CmdThread):
    def __init__(self, spsCmd, specNum, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'enu_sm{specNum}', cmdStr=cmdStr, timeLim=timeLim)


class BiaCmd(sync.CmdThread):
    def __init__(self, spsCmd, specNum, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'enu_sm{specNum}', cmdStr=cmdStr, timeLim=timeLim)


class CcdMotorsMoveCmd(sync.CmdThread):
    def __init__(self, spsCmd, cam, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'xcu_{cam}', cmdStr=cmdStr, timeLim=timeLim)