import os

import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.opdb import opDB
from ics.utils.threading import singleShot
from ics.utils.threading import threaded
from spsActor.utils.ids import SpsIds as idsUtils


def getExposureInfo(filepath):
    """Retrieve info from filepath."""
    __, filename = os.path.split(filepath)
    filename, __ = os.path.splitext(filename)
    visit = int(filename[4:10])
    specNum = int(filename[-2])
    armNum = int(filename[-1])
    return visit, specNum, armNum


class HxExposure(QThread):
    # timeout setting.
    """Placeholder to handle hxActor cmd threading."""

    def __init__(self, exp, cam):
        """Parameters
        ----------
        exp : `spsActor.utils.exposure.Exposure`
           exposure object.
        cam : `
           camera object.
        """

        def nRead(exp):
            """Calculate number of read given exptype, exptime."""
            if exp.coreExpType == 'bias':
                # bias does not mean anything for H4, could maybe mean a single read ? not doing it for now.
                nRead = 0
            elif exp.coreExpType == 'dark':
                # signal = ramp[-1] - ramp[0] , since ramp[0] is only use to subtract, you need an extra-one.
                nRead = round(exp.exptime / self.readTime) + 1
            else:
                # signal = ramp[-1] - ramp[0], you need a clean ramp[0] that will be subtracted, and you need an extra
                # after the shutter/lamp transition.
                # In other words you always need to bracket your signal with clean/stable ramps.
                nRead = (exp.exptime + exp.expTimeOverHead) // self.readTime + 3

            return int(nRead)

        self.exp = exp
        self.cam = cam
        self.hx = f'hx_{cam}'

        QThread.__init__(self, self.exp.actor, self.hx)
        QThread.start(self)

        self.doFinalize = False
        self.clearASAP = False

        self.wipedAt = None
        self.rampVar = None
        self.readVar = None

        # be nice and initialize those variables
        self.time_exp_end = None
        self.exptime = -9998.0
        self.dateobs = 'None'

        self.state = 'none'
        self.readTime = float(exp.actor.models[self.hx].keyVarDict['readTime'].getValue())
        # differentiating between the original number of read (nRead0) and current number of read(nRead).
        self.nRead = self.nRead0 = nRead(exp)

        # add callback for shutters state, useful to fire process asynchronously.
        self.hxRead = exp.actor.models[self.hx].keyVarDict['hxread']
        self.hxRead.addCallback(self.hxReadCB)

        self.filename = exp.actor.models[self.hx].keyVarDict['filename']
        self.filename.addCallback(self.newFileNameCB)

        # gotcha to pretend this is a ccd.
        self.read = self.keepShutterKeys
        # there is no concept of clearing / aborting, so just do a final read.
        self.abort = self.finish = self.clearExposure = self.finishRampASAP

    @property
    def exptype(self):
        return self.exp.exptype

    @property
    def storable(self):
        return self.readVar is not None

    @property
    def isFinished(self):
        return self.storable or self.cleared or self.nRead0 == 0

    @property
    def cleared(self):
        return self.rampVar is not None and (self.rampVar.didFail or self.clearASAP)

    @property
    def wiped(self):
        return self.wipedAt is not None

    @property
    def preparingForShutterOpen(self):
        return self.state in ['none', 'reset']

    def hxReadCB(self, keyVar):
        """H4 read callback."""
        visit, nRamp, nGroup, nRead = keyVar.getValue(doRaise=False)

        # no need to go further.
        if visit != self.exp.visit:
            return

        # track h4 state.
        self.actor.bcast.debug(f'text="{self.hx} {visit} {nRamp} {nGroup} {nRead}"')

        if nGroup == 0:
            self.state = 'reset'

        # pretending this is a ccd.
        if nGroup == 1 and nRead == 1:
            self.wipedAt = pfsTime.timestamp()
            self.state = 'integrating'

        elif nGroup == 1 and nRead == self.nRead:
            self.state = 'idle'

        # finishRamp already sent.
        if self.clearASAP:
            return

        doFinalize = self.doFinalize and nRead < self.nRead  # it is too late otherwise in any-case.

        if doFinalize:
            doStop = nRead < (self.nRead - 1)
            # if doStop set nRead to the next one.
            if doStop:
                self.nRead = nRead + 1

            self._finishRamp(self.exp.cmd, doStop=doStop)

    def newFileNameCB(self, keyVar):
        """H4 callback when filename gets generated."""
        filepath = keyVar.getValue(doRaise=False)

        # no need to go further
        if filepath is None:
            return

        visit, __, __ = getExposureInfo(filepath)

        # no need to go further.
        if visit != self.exp.visit:
            return

        # For regular exposure, finalize is called whenever the shutters close
        # so way before the filepath is generated
        # But not for darks, so it needs to be done here.
        if not self.time_exp_end:
            dateobs = pfsTime.convert.datetime_to_isoformat(pfsTime.convert.datetime_from_timestamp(self.wipedAt))
            self.keepShutterKeys(None, visit, dateobs=dateobs, exptime=self.nRead0 * self.readTime)

        self.readVar = keyVar

    def finishRampASAP(self, cmd):
        """Finish ramp as soon as possible."""
        # meaning shutters has been used.
        if self.doFinalize:
            return

        # whenever the ramp command returns, exposure is considered cleared.
        self.clearASAP = True
        return self._finishRamp(self.exp.cmd, doStop=True)

    def _ramp(self, cmd, expectedExptime=0):
        """Send h4 ramp command and handle reply."""
        expectedExptime = f'expectedExptime={expectedExptime}' if expectedExptime else ''
        cmdStr = f'ramp nread={self.nRead0} visit={self.exp.visit} exptype={self.exptype} {expectedExptime}'.strip()

        self.rampVar = self.actor.crudeCall(cmd, actor=self.hx, cmdStr=cmdStr,
                                            timeLim=(self.nRead0 + 2) * self.readTime + 60)

        if self.rampVar.didFail:
            raise exception.HxRampFailed(self.hx, cmdUtils.interpretFailure(self.rampVar))

    @threaded
    def ramp(self, cmd, expectedExptime):
        """Start h4 ramp."""
        try:
            self._ramp(cmd, expectedExptime=expectedExptime)
        except exception.HxRampFailed as e:
            self.exp.abort(cmd, reason=str(e))

    @threaded
    def expose(self, cmd, visit):
        """Full exposure routine for calib object. """
        # no need to go further.
        if not self.nRead0:
            return

        try:
            self._ramp(cmd)
        except Exception as e:
            self.exp.abort(cmd, reason=str(e))

    def declareFinalRead(self, cmd=None):
        """Declare that the next read will be the final one."""
        self.actor.logger.info(f'{self.hx} will be asked to finishRamp when next read is done')
        self.doFinalize = True  # actually the only way to reach the main thread.

    def startAndWaitForReset(self, cmd):
        """Minor optimization to start wiping ccds when reset frame is done?. """
        # I have to pass the expected exposure time to hx.ramp(), will be overriden with _rampFinish.
        self.ramp(cmd, expectedExptime=self.exp.exptime)

        # sm1 wipe is too slow for this optimization for now, so skip it.
        if str(self.cam) == 'n1':
            return

        while not self.reset:
            pfsTime.sleep.millisec()
            # if the hx.ramp() fails you want to escape that loop.
            if self.cleared:
                raise exception.ExposureAborted

            if self.exp.doFinish or self.exp.doAbort:
                self.declareFinalRead()

    @singleShot
    def _finishRamp(self, cmd, doStop):
        """Finish ramp, which will gather the final fits keys."""
        stopRamp = 'stopRamp' if doStop else ''
        cmdStr = f'ramp finish exptime={self.exptime} obstime={self.dateobs} {stopRamp}'.strip()

        cmdVar = self.actor.crudeCall(cmd, actor=self.hx, cmdStr=cmdStr, timeLim=60)
        self.actor.logger.info(f'{self.hx} ramp finish didFail({cmdVar.didFail})')

    def keepShutterKeys(self, cmd, visit, dateobs, exptime):
        """Keep exposure info from the shutters."""
        self.dateobs = dateobs
        self.exptime = round(exptime, 3)
        self.time_exp_end = pfsTime.timestamp()

    def store(self):
        """Store in sps_exposure in opDB database."""
        if not self.storable:
            return

        filepath = self.readVar.getValue(doRaise=False)
        visit, specNum, armNum = getExposureInfo(filepath)

        cam = idsUtils.camFromNums(specNum=specNum, armNum=armNum)

        # convert time_exp_start to datetime object.
        time_exp_start = pfsTime.Time.fromisoformat(self.dateobs).to_datetime()
        # convert timestamp to datetime object.
        time_exp_end = pfsTime.Time.fromtimestamp(self.time_exp_end).to_datetime()
        # invalid for now
        beamConfigDate = 9998.0

        try:
            opDB.insert('sps_exposure',
                        pfs_visit_id=int(visit), sps_camera_id=cam.camId, exptime=self.exptime,
                        time_exp_start=time_exp_start, time_exp_end=time_exp_end,
                        beam_config_date=float(beamConfigDate))
            return cam.camName
        except Exception as e:
            self.actor.bcast.warn('text=%s' % self.actor.strTraceback(e))

    def handleTimeout(self):
        """Just a prototype."""
        pass

    def exit(self):
        """Overriding QThread.exit(self)."""
        self.hxRead.removeCallback(self.hxReadCB)
        self.filename.removeCallback(self.newFileNameCB)
        QThread.exit(self)
