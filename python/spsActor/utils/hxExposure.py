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
        self.exp = exp
        self.cam = cam
        self.hx = f'hx_{cam}'
        self.nRead = int(self.exp.exptime // HxExposure.rampTime + 2)

        QThread.__init__(self, self.exp.actor, self.hx)
        QThread.start(self)

        self.state = 'none'
        self.wiped = False
        self.cleared = False
        self.readVar = None

        # add callback for shutters state, useful to fire process asynchronously.
        self.hxRead = exp.actor.models[self.hx].keyVarDict['hxread']
        self.hxRead.addCallback(self.hxReadCB)

        self.filename = exp.actor.models[self.hx].keyVarDict['filename']
        self.filename.addCallback(self.newFileNameCB)

        # gotcha to pretend this is a ccd.
        self.wipe = self.ramp
        self.read = self.shutterIsClosed

    @property
    def exptype(self):
        return self.exp.exptype

    @property
    def isFinished(self):
        return self.cleared or self.storable

    @property
    def storable(self):
        return self.readVar is not None

    def hxReadCB(self, keyVar):
        """H4 read callback."""
        visit, nRamp, nGroup, nRead = keyVar.getValue(doRaise=False)

        # no need to go further.
        if visit != self.exp.visit:
            return

        # track h4 state.
        self.actor.bcast.debug(f'text="{self.hx} {visit} {nRamp} {nGroup} {nRead}"')

        # pretending this is a ccd.
        if nGroup == 0:
            self.wiped = True
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

    def shutterIsClosed(self, cmd, visit, dateobs, exptime):
        """Called from specModule thread, after shutters are closed."""
        self.dateobs = dateobs
        self.exptime = round(exptime, 3)
        self.time_exp_end = pfsTime.timestamp()

    def abort(self, cmd):
        """Just a prototype."""
        pass

    def finish(self, cmd):
        """Just a prototype."""
        pass

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
