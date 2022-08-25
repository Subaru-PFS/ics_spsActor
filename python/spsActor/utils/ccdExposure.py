import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.opdb import opDB
from ics.utils.threading import threaded
from spsActor.utils.ids import SpsIds as idsUtils


class CcdExposure(QThread):
    # timeout setting.
    wipeTimeLim = 30
    readTimeLim = 90
    clearTimeLim = 10
    """ Placeholder to handle ccdActor cmd threading """

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
        self.ccd = f'ccd_{cam}'

        self.states = []
        self.readVar = None
        self.cleared = None

        QThread.__init__(self, self.exp.actor, self.ccd)
        QThread.start(self)

        # add callback for shutters state, useful to fire process asynchronously.
        self.stateKeyVar = exp.actor.models[self.ccd].keyVarDict['exposureState']
        self.stateKeyVar.addCallback(self.exposureState)

    @property
    def exptype(self):
        return self.exp.exptype

    @property
    def wipeFlavour(self):
        return self.exp.wipeFlavour[self.cam.arm]

    @property
    def readFlavour(self):
        return self.exp.readFlavour[self.cam.arm]

    @property
    def state(self):
        if self.cleared:
            return 'cleared'

        return self.exp.actor.models[self.ccd].keyVarDict['exposureState'].getValue(doRaise=False)

    @property
    def storable(self):
        return self.readVar is not None

    @property
    def isFinished(self):
        return self.cleared or self.storable

    @property
    def wiped(self):
        return 'wiping' in self.states and 'integrating' in self.states

    def exposureState(self, keyVar):
        """Exposure State callback."""
        state = keyVar.getValue(doRaise=False)
        # track ccd state.
        self.actor.bcast.debug(f'text="{self.ccd} {state}"')
        self.states.append(state)

    def _wipe(self, cmd):
        """ Send ccd wipe command and handle reply """
        cmdVar = self.actor.crudeCall(cmd, actor=self.ccd, cmdStr=f'wipe {self.wipeFlavour}',
                                      timeLim=CcdExposure.wipeTimeLim)
        if cmdVar.didFail:
            raise exception.WipeFailed(self.ccd, cmdUtils.interpretFailure(cmdVar))

        return pfsTime.timestamp()

    def _read(self, cmd, visit, dateobs, exptime=None):
        """ Send ccd read command and handle reply. """
        self.dateobs = dateobs
        self.time_exp_end = pfsTime.timestamp()

        darktime = round(self.time_exp_end - self.wipedAt, 3)
        exptime = darktime if exptime is None else exptime
        exptime = round(exptime, 3)

        cmdVar = self.actor.crudeCall(cmd, actor=self.ccd, cmdStr=f'read {self.exptype} '
                                                                  f'visit={visit} exptime={exptime} '
                                                                  f'darktime={darktime} obstime={dateobs} '
                                                                  f'{self.readFlavour}',
                                      timeLim=CcdExposure.readTimeLim)

        if cmdVar.didFail:
            raise exception.ReadFailed(self.ccd, cmdUtils.interpretFailure(cmdVar))

        self.readVar = cmdVar
        return exptime

    def integrate(self):
        """ Integrate for exptime in seconds, doFinish==doAbort at the beginning of integration. """
        if self.exp.doFinish:
            raise exception.EarlyFinish
        if self.exp.doAbort:
            raise exception.ExposureAborted

        integrationEnd = self.wipedAt + self.exp.exptime

        while pfsTime.timestamp() < integrationEnd:
            if self.exp.doAbort:
                raise exception.ExposureAborted
            if self.exp.doFinish:
                break

            pfsTime.sleep.millisec()

        # convert timestamp to localized datetime.
        dateobs = pfsTime.convert.datetime_from_timestamp(self.wipedAt)
        # dateobs is actually a string to be fast and consistent with expose.
        return pfsTime.convert.datetime_to_isoformat(dateobs)

    def clearExposure(self, cmd):
        """ Call ccdActor clearExposure command """
        if self.cleared is None:
            self.cleared = False
            self.actor.safeCall(cmd, actor=self.ccd, cmdStr='clearExposure', timeLim=CcdExposure.clearTimeLim)
            self.cleared = True

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine for calib object. """
        try:
            self.wipedAt = self._wipe(cmd)
            dateobs = self.integrate()
            self.exptime = self._read(cmd, visit, dateobs)

        except Exception as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    @threaded
    def wipe(self, cmd):
        """ Wipe in thread. """
        try:
            self.wipedAt = self._wipe(cmd)
        except exception.WipeFailed as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    @threaded
    def read(self, cmd, visit, dateobs, exptime):
        """ Read in thread. """
        try:
            self.exptime = self._read(cmd, visit, dateobs, exptime)
        except exception.ReadFailed as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    def store(self):
        """ Store in sps_exposure in opDB database. """
        if not self.storable:
            return

        keys = cmdUtils.cmdVarToKeys(cmdVar=self.readVar)
        visit, beamConfigDate = keys['beamConfigDate'].values
        camStr, dateDir, visit, specNum, armNum = keys['spsFileIds'].values
        cam = idsUtils.camFromNums(specNum=specNum, armNum=armNum)

        # convert time_exp_start to datetime object.
        time_exp_start = pfsTime.Time.fromisoformat(self.dateobs).to_datetime()
        # convert timestamp to datetime object.
        time_exp_end = pfsTime.Time.fromtimestamp(self.time_exp_end).to_datetime()

        try:
            opDB.insert('sps_exposure',
                        pfs_visit_id=int(visit), sps_camera_id=cam.camId, exptime=self.exptime,
                        time_exp_start=time_exp_start, time_exp_end=time_exp_end,
                        beam_config_date=float(beamConfigDate))
            return cam.camName
        except Exception as e:
            self.actor.bcast.warn('text=%s' % self.actor.strTraceback(e))

    def abort(self, cmd):
        """ Just a prototype. """
        pass

    def finish(self, cmd):
        """ Just a prototype. """
        pass

    def handleTimeout(self):
        """ Just a prototype. """
        pass

    def exit(self):
        """Overriding QThread.exit(self)"""
        self.stateKeyVar.removeCallback(self.exposureState)
        QThread.exit(self)
