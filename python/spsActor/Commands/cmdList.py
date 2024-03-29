import ics.utils.cmd as cmdUtils
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


class SlitStart(sync.SpsCmd):
    timeLim = 120

    def __init__(self, spsActor, specNums):
        sync.SpsCmd.__init__(self, spsActor)
        self.attachThreads([SlitStartCmd(self, specNum, 'slit start', SlitStart.timeLim) for specNum in specNums])


class SlitStop(sync.SpsCmd):
    timeLim = 30

    def __init__(self, spsActor, specNums):
        sync.SpsCmd.__init__(self, spsActor)
        self.attachThreads([SlitStopCmd(self, specNum, 'slit stop', SlitStop.timeLim) for specNum in specNums])


class BiaSwitch(sync.SpsCmd):
    timeLim = 10

    def __init__(self, spsActor, specNums, state, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'bia {state}', **kwargs)
        self.attachThreads([BiaCmd(self, specNum, cmdStr, BiaSwitch.timeLim) for specNum in specNums])


class IisOn(sync.SpsCmd):
    timeLim = 90

    def __init__(self, spsActor, specNums, warmingTime=False, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        timeLim = warmingTime + 30 if warmingTime else IisOn.timeLim
        cmdStr = cmdUtils.parse(f'iis', warmingTime=warmingTime, **kwargs)
        self.attachThreads([IisCmd(self, specNum, cmdStr, timeLim) for specNum in specNums])


class IisOff(sync.SpsCmd):
    timeLim = 10

    def __init__(self, spsActor, specNums, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'iis', **kwargs)
        self.attachThreads([IisCmd(self, specNum, cmdStr, IisOff.timeLim) for specNum in specNums])


class IisPrepare(sync.SpsCmd):
    timeLim = 10

    def __init__(self, spsActor, specNums, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'iis prepare', **kwargs)
        self.attachThreads([IisCmd(self, specNum, cmdStr, IisPrepare.timeLim) for specNum in specNums])


class FpaMove(sync.SpsCmd):
    timeLim = 30

    def __init__(self, spsActor, cams, cmdHead, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        cmdStr = cmdUtils.parse(f'motors {cmdHead}', **kwargs)
        self.attachThreads([FpaMoveCmd(self, cam, cmdStr, FpaMove.timeLim) for cam in cams])


class CcdErase(sync.SpsCmd):
    timeLim = 30

    def __init__(self, spsActor, cams, **kwargs):
        sync.SpsCmd.__init__(self, spsActor)
        self.attachThreads([CcdEraseCmd(self, cam, 'erase', CcdErase.timeLim) for cam in cams])


class EnuThread(sync.CmdThread):
    def __init__(self, spsCmd, specNum, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'enu_sm{specNum}', cmdStr=cmdStr, timeLim=timeLim)


class XcuThread(sync.CmdThread):
    def __init__(self, spsCmd, cam, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'xcu_{cam}', cmdStr=cmdStr, timeLim=timeLim)


class CcdThread(sync.CmdThread):
    def __init__(self, spsCmd, cam, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'ccd_{cam}', cmdStr=cmdStr, timeLim=timeLim)


class HxThread(sync.CmdThread):
    def __init__(self, spsCmd, cam, cmdStr, timeLim):
        sync.CmdThread.__init__(self, spsCmd,
                                actor=f'hx_{cam}', cmdStr=cmdStr, timeLim=timeLim)


class RdaMoveCmd(EnuThread):
    """"""


class SlitMoveCmd(EnuThread):
    """"""


class SlitStartCmd(EnuThread):
    """"""


class SlitStopCmd(EnuThread):
    """"""


class BiaCmd(EnuThread):
    """"""


class IisCmd(EnuThread):
    """"""


class FpaMoveCmd(XcuThread):
    """"""


class CcdEraseCmd(CcdThread):
    """"""
