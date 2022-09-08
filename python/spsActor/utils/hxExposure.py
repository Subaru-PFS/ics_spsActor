import os

import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.opdb import opDB
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
    rampTime = 10.857
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
            if exp.exptype == 'bias':
                # bias does not mean anything for H4, could maybe mean a single read ? not doing it for now.
                nRead = 0
            elif exp.exptype == 'dark':
                # signal = ramp[-1] - ramp[0] , since ramp[0] is only use to subtract, you need an extra-one.
                nRead = round(exp.exptime / HxExposure.rampTime) + 1
            else:
                # signal = ramp[-1] - ramp[0], you need a clean ramp[0] that will be subtracted, and you need an extra
                # after the shutter/lamp transition.
                # In other words you always need to bracket your signal with clean/stable ramps.
                nRead = exp.exptime // HxExposure.rampTime + 3

            return int(nRead)

        self.exp = exp
        self.cam = cam
        self.hx = f'hx_{cam}'

        QThread.__init__(self, self.exp.actor, self.hx)
        QThread.start(self)

        self.reset = False
        self.wipedAt = None
        self.readVar = None
        self.cleared = None

        self.state = 'none'
        self.nRead = nRead(exp)

        # add callback for shutters state, useful to fire process asynchronously.
        self.hxRead = exp.actor.models[self.hx].keyVarDict['hxread']
        self.hxRead.addCallback(self.hxReadCB)

        self.filename = exp.actor.models[self.hx].keyVarDict['filename']
        self.filename.addCallback(self.newFileNameCB)

        # gotcha to pretend this is a ccd.
        self.read = self.finalize

    @property
    def exptype(self):
        return self.exp.exptype

    @property
    def storable(self):
        return self.readVar is not None

    @property
    def isFinished(self):
        return self.cleared or self.storable or self.nRead == 0

    @property
    def wiped(self):
        return self.wipedAt is not None

    def hxReadCB(self, keyVar):
        """H4 read callback."""
        visit, nRamp, nGroup, nRead = keyVar.getValue(doRaise=False)

        # no need to go further.
        if visit != self.exp.visit:
            return

        # track h4 state.
        self.actor.bcast.debug(f'text="{self.hx} {visit} {nRamp} {nGroup} {nRead}"')

        if nGroup == 0:
            self.reset = True

        # pretending this is a ccd.
        if nGroup == 1 and nRead == 1:
            self.wipedAt = pfsTime.timestamp()
            self.state = 'integrating'

        elif nGroup == 1 and nRead == self.nRead:
            self.state = 'idle'

    def newFileNameCB(self, keyVar):
        """H4 callback when filename gets generated."""
        filepath = keyVar.getValue(doRaise=False)
        visit, __, __ = getExposureInfo(filepath)

        # no need to go further.
        if visit != self.exp.visit:
            return

        self.readVar = keyVar

    def _ramp(self, cmd):
        """Send h4 ramp command and handle reply."""
        cmdVar = self.actor.crudeCall(cmd, actor=self.hx, cmdStr=f'ramp nread={self.nRead} visit={self.exp.visit}',
                                      timeLim=(self.nRead + 1) * HxExposure.rampTime + 30)
        if cmdVar.didFail:
            raise exception.HxRampFailed(self.hx, cmdUtils.interpretFailure(cmdVar))

    @threaded
    def ramp(self, cmd):
        """Start h4 ramp."""
        try:
            self._ramp(cmd)
        except exception.HxRampFailed as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine for calib object. """
        # no need to go further.
        if not self.nRead:
            return

        try:
            self._ramp(cmd)
            self.finalize(cmd, visit,
                          dateobs=pfsTime.convert.datetime_to_isoformat(
                              pfsTime.convert.datetime_from_timestamp(self.wipedAt)),
                          exptime=self.nRead * HxExposure.rampTime)

        except Exception as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    def finalize(self, cmd, visit, dateobs, exptime):
        """Called to finalize exposure, from specModule thread, after shutters are closed for example."""
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

    def abort(self, cmd):
        """Just a prototype."""
        pass

    def finish(self, cmd):
        """Just a prototype."""
        pass

    def clearExposure(self, cmd):
        """Just a prototype."""
        self.cleared = True

    def handleTimeout(self):
        """Just a prototype."""
        pass

    def exit(self):
        """Overriding QThread.exit(self)."""
        self.hxRead.removeCallback(self.hxReadCB)
        self.filename.removeCallback(self.newFileNameCB)
        QThread.exit(self)
