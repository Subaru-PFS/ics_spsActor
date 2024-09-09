import os

import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import numpy as np
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
                # EDIT APRIL24 : to be able to synchronise h4 safely, an extra-read was added.
                nReadMin = exp.rampConfig['nReadMin'] + exp.rampConfig['nExtraRead']
                nRead = (exp.exptime + exp.expTimeOverHead) // self.readTime + nReadMin

            return int(nRead)

        self.exp = exp
        self.cam = cam
        self.hx = f'hx_{cam}'

        QThread.__init__(self, self.exp.actor, self.hx)
        QThread.start(self)

        self.doFinalize = False
        self.clearASAP = False
        self.waitForRampCmdReturn = True

        self.wipedAt = None
        self.rampVar = None
        self.readVar = None
        self.rampTiming = dict(maxResetEndTime=np.inf)

        # be nice and initialize those variables
        self.time_exp_end = None
        self.exptime = None
        self.dateobs = None

        self.states = ['none']
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
        # Nota bene June24 : abort, finish are not just a placeholder, it is required.
        self.abort = self.finish = self.clearExposure = self.finishRampASAP

    @property
    def exptype(self):
        return self.exp.exptype

    @property
    def storable(self):
        return self.readVar is not None

    @property
    def isFinished(self):
        return self.rampVar is not None or self.cleared or not self.nRead0

    @property
    def cleared(self):
        return self.clearASAP and (self.rampVar is not None or not self.waitForRampCmdReturn)

    @property
    def firstReadDone(self):
        return 'integrating' in self.states

    @property
    def wiped(self):
        return self.firstReadDone

    @property
    def state(self):
        return self.states[-1]

    def calculateRampTiming(self):
        """
        Calculate timing details for ramp operations with an overhead.

        Returns:
        dict: Contains start ramp time, max reset duration, max first read duration,
              max reset end time, and max first read end time.
        """
        # empirically adding 5 seconds overhead.
        overHead = 5
        startRamp = pfsTime.timestamp()
        maxResetDuration = int(round(2 * self.readTime + overHead))
        maxFirstReadDuration = int(round(3 * self.readTime + overHead))
        maxResetEndTime = startRamp + maxResetDuration
        maxFirstReadEndTime = startRamp + maxFirstReadDuration
        return self.rampTiming.update(maxResetDuration=maxResetDuration, maxFirstReadDuration=maxFirstReadDuration,
                                      maxResetEndTime=maxResetEndTime, maxFirstReadEndTime=maxFirstReadEndTime,
                                      startRamp=startRamp)

    def checkResetTiming(self):
        """
        Check if the reset timing has exceeded the maximum allowed duration.

        Raises:
        exception.HxRampFailed: If the state is not 'reset' and the reset duration has exceeded the limit.
        """
        if self.state != 'reset' and pfsTime.timestamp() > self.rampTiming['maxResetEndTime']:
            self.waitForRampCmdReturn = False
            raise exception.HxRampFailed(self.hx,
                                         f'was not reset after {self.rampTiming["maxResetDuration"]} seconds')

    def checkFirstReadTiming(self):
        """
        Check if the first read timing has exceeded the maximum allowed duration.

        Raises:
        exception.HxRampFailed: If the first read is not done and the duration has exceeded the limit.
        """
        if not self.firstReadDone and pfsTime.timestamp() > self.rampTiming['maxFirstReadEndTime']:
            self.waitForRampCmdReturn = False
            raise exception.HxRampFailed(self.hx,
                                         f'did not reach first read after {self.rampTiming["maxFirstReadDuration"]} seconds')

    def hxReadCB(self, keyVar):
        """H4 read callback, called at the end the read."""
        visit, nRamp, nGroup, nRead = keyVar.getValue(doRaise=False)

        # no need to go further.
        if visit != self.exp.visit:
            return

        # track h4 state.
        self.actor.bcast.debug(f'text="{self.hx} {visit} {nRamp} {nGroup} {nRead}"')

        if nGroup == 0:
            self.states.append('reset')

        # pretending this is a ccd.
        elif nGroup == 1 and nRead == 1:
            self.wipedAt = pfsTime.timestamp()
            self.states.append('integrating')

        elif nGroup == 1 and nRead == self.nRead:
            self.states.append('idle')

        # finishRamp(doStop=True) already sent from finishASAP.
        if self.clearASAP:
            return

        doFinalize = self.doFinalize and nRead < self.nRead  # it is too late otherwise in any-case.

        if doFinalize:
            doStop = nRead < (self.nRead - (1 + self.exp.rampConfig['nExtraRead']))
            # if doStop set nRead to the next one.
            if doStop:
                self.nRead = nRead + 1

            self._finishRamp(self.exp.cmd, doStop=doStop)
            self.doFinalize = False

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
            # should never happen, but still covering that case.
            if not self.wipedAt:
                self.actor.logger.warning(f'{self.hx} filename was generated but first read were never declared...')
                self.wipedAt = pfsTime.timestamp()

            dateobs = pfsTime.convert.datetime_to_isoformat(pfsTime.convert.datetime_from_timestamp(self.wipedAt))
            self.keepShutterKeys(None, visit, dateobs=dateobs, exptime=self.nRead0 * self.readTime)

        self.readVar = keyVar

    def finishRampASAP(self, cmd):
        """Finish ramp as soon as possible."""
        # meaning shutters has been used, ramp will already be told to finish at next read.
        if self.doFinalize or self.clearASAP:
            return

        # whenever the ramp command returns, exposure is considered cleared.
        self.clearASAP = True
        return self._finishRamp(self.exp.cmd, doStop=True)

    def _ramp(self, cmd, expectedExptime=0):
        """Send h4 ramp command and handle reply."""
        cmdParams = dict(nread=self.nRead0, visit=self.exp.visit,
                         pfsDesign=f'0x{self.exp.designId:016x},"{self.exp.designName}"',
                         exptype=self.exptype)
        if expectedExptime:
            cmdParams["expectedExptime"] = expectedExptime

        # calculate time limit for reset time and wipe time.
        self.calculateRampTiming()

        self.rampVar = self.actor.crudeCall(cmd, actor=self.hx, cmdStr=cmdUtils.parse('ramp', **cmdParams),
                                            timeLim=(self.nRead0 + 2) * self.readTime + 90)

        if self.rampVar.didFail:
            raise exception.HxRampFailed(self.hx, cmdUtils.interpretFailure(self.rampVar))

        if self.rampVar and not self.readVar:
            raise exception.HxRampFailed(self.hx, 'ramp command finished but filename was not generated ...')

    @threaded
    def ramp(self, cmd, expectedExptime):
        """Start h4 ramp."""
        try:
            self._ramp(cmd, expectedExptime=expectedExptime)
        except Exception as e:
            self.handleRampFailed(cmd, reason=str(e))

    @threaded
    def expose(self, cmd, visit):
        """Full exposure routine for calib object. """
        # no need to go further.
        if not self.nRead0:
            return

        try:
            self._ramp(cmd)
        except Exception as e:
            self.handleRampFailed(cmd, reason=str(e))

    def handleRampFailed(self, cmd, reason):
        """Handle ramp failure."""
        if self.firstReadDone:
            self.exp.failures.add(reason=reason)  # just report the failure, but proceed with the rest of the camera.
        else:
            self.exp.abort(cmd, reason=reason)  # early failure, report and abort right away.

    def declareFinalRead(self, cmd=None):
        """Declare that the next read will be the final one."""
        self.actor.logger.info(f'{self.hx} will be asked to finishRamp when next read is done')
        self.doFinalize = True  # actually the only way to reach the main thread.

    @singleShot
    def _finishRamp(self, cmd, doStop):
        """Finish ramp, which will gather the final fits keys."""
        if self.rampVar and self.rampVar.didFail:
            return

        exptime = f'exptime={self.exptime} ' if self.exptime else ''
        obstime = f'obstime={self.dateobs} ' if self.dateobs else ''
        stopRamp = 'stopRamp' if doStop else ''
        # parsing arguments.
        cmdStr = f'ramp finish {exptime}{obstime}{stopRamp}'.strip()

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
