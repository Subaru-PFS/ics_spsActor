from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from opdb import utils, opdb
from spsActor.utils.lib import cmdVarToKeys, camPerSpec, wait, threaded, fromisoformat, interpretFailure


class SpecModuleExposure(QThread):
    """ Placeholder to handle spectograph module cmd threading. """

    def __init__(self, exp, smId, arms):
        self.exp = exp
        self.smId = smId
        self.arms = arms
        self.specName = f'sm{smId}'
        self.enu = f'enu_{self.specName}'
        self.camExp = [CcdExposure(exp, f'{arm}{smId}') for arm in arms]

        QThread.__init__(self, exp.actor, self.specName)
        self.start()

    @property
    def runExp(self):
        return list(set(self.camExp) - set(self.clearedExp))

    @property
    def clearedExp(self):
        return [camExp for camExp in self.camExp if camExp.cleared]

    @property
    def isFinished(self):
        return all(camExp.isFinished for camExp in self.camExp)

    def currently(self, state):
        """ current camExp states  """
        return [camExp.state == state for camExp in self.runExp]

    def getShutters(self):
        """ Build argument to enu shutters expose cmd. """
        return '' if 'b' in self.arms else 'red'

    def wipe(self, cmd):
        """ Wipe running CcdExposure and wait for integrating state.
        doFinish==doAbort at the beginning of integration.
        """
        for camExp in self.runExp:
            camExp.wipe(cmd)

        while not any(self.currently(state='wiping')):
            wait()

        while not all(self.currently(state='integrating')):
            wait()

        if any(self.clearedExp):
            raise exception.StopExposureASAP

        if self.exp.doFinish:
            self.exp.failures.add("ExposureAborted(doFinish requested before exposing)")

        if self.exp.doAbort or self.exp.doFinish:
            raise exception.StopExposureASAP

    def integrate(self, cmd, shutterTime=None):
        """ Integrate for both calib and regular exposure """

        shutterTime = self.exp.exptime if shutterTime is None else shutterTime

        shutters = self.getShutters()
        cmdVar = self.exp.actor.crudeCall(cmd, actor=self.enu, cmdStr=f'shutters expose {shutters}',
                                          exptime=shutterTime, timeLim=shutterTime + 2)
        if self.exp.doAbort:
            raise exception.StopExposureASAP

        if cmdVar.didFail:
            raise exception.ShuttersFailed(self.specName, interpretFailure(cmdVar))

        keys = cmdVarToKeys(cmdVar)

        exptime = float(keys['exptime'].values[0])
        dateobs = fromisoformat(keys['dateobs'].values[0])

        return exptime, dateobs

    def read(self, cmd, visit, exptime, dateobs):
        """ Read running CcdExposure and wait for idle state. """
        for camExp in self.runExp:
            camExp.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        while not all(self.currently(state='idle')):
            wait()

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine, exceptions are catched and handled under the cover. """

        try:
            self.wipe(cmd)
            exptime, dateobs = self.integrate(cmd)
            self.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        except exception.StopExposureASAP:
            self.clearExposure(cmd)

        except exception.ShuttersFailed as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    def clearExposure(self, cmd):
        """ Clear all running CcdExposure. """
        for camExp in self.runExp:
            camExp.clearExposure(cmd)

    def abort(self, cmd):
        """ Command shutters to abort exposure. """
        if any(self.currently(state='integrating')):
            self.exp.actor.safeCall(cmd, actor=self.enu, cmdStr='exposure finish')

    def finish(self, cmd):
        """ Command shutters to finish exposure. """
        if any(self.currently(state='integrating')):
            self.exp.actor.safeCall(cmd, actor=self.enu, cmdStr='exposure finish')

    def exit(self):
        """ Free up all resources """
        for camExp in self.camExp:
            camExp.exit()

        self.camExp.clear()
        QThread.exit(self)


class Exposure(object):
    """ Exposure object. """
    SpecModuleExposureClass = SpecModuleExposure
    def __init__(self, actor, exptype, exptime, cams, doTest=False):
        exptype = 'test' if doTest else exptype
        self.doAbort = False
        self.doFinish = False
        self.actor = actor
        self.exptype = exptype
        self.exptime = exptime
        self.failures = exception.Failures()

        self.smThreads = self.instantiate(cams)

    @property
    def camExp(self):
        return sum([th.camExp for th in self.smThreads], [])

    @property
    def isFinished(self):
        return all([th.isFinished for th in self.smThreads])

    @property
    def storable(self):
        return any([camExp.storable for camExp in self.camExp])

    @property
    def clearedExp(self):
        return [camExp for camExp in self.camExp if camExp.cleared]

    @property
    def threads(self):
        return self.smThreads

    def instantiate(self, cams):
        """ Create underlying specModuleExposure threads.  """
        return [self.SpecModuleExposureClass(self, smId, cams) for smId, cams in camPerSpec(cams).items()]

    def abort(self, cmd, reason="ExposureAborted()"):
        """ Abort current exposure. """
        self.doAbort = True
        self.failures.add(reason)

        for thread in self.threads:
            thread.abort(cmd)

    def finish(self, cmd):
        """ Finish current exposure. """
        self.doFinish = True
        for thread in self.threads:
            thread.finish(cmd)

    def start(self, cmd, visit):
        """ Start all spectrograph module exposures. """
        for thread in self.smThreads:
            thread.expose(cmd, visit)

    def exit(self):
        """ Free up all resources. """
        for thread in self.threads:
            thread.exit()

        self.threads.clear()

    def store(self, cmd, visit):
        """Store Exposure in sps_visit table in opdb database. """
        try:
            utils.insert(opdb.OpDB.url, 'sps_visit',
                         pd.DataFrame(dict(pfs_visit_id=visit, exp_type=self.exptype), index=[0]))
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))

        frames = [camExp.store() for camExp in self.camExp]
        return list(filter(None, frames))


class DarkExposure(Exposure):
    """ CaliDarkExposureb object. """

    def __init__(self, *args, **kwargs):
        Exposure.__init__(self, *args, **kwargs)

    @property
    def camExp(self):
        return self.threads

    def instantiate(self, cams):
        """ Create underlying CcdExposure threads object. """
        return [CcdExposure(self, cam) for cam in cams]


class CcdExposure(QThread):
    """ Placeholder to handle ccdActor cmd threading """

    def __init__(self, exp, cam):
        self.exp = exp
        self.exptype = exp.exptype
        self.ccd = f'ccd_{cam}'

        self.readVar = None
        self.cleared = None

        QThread.__init__(self, self.exp.actor, self.ccd)
        QThread.start(self)

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

    def _wipe(self, cmd):
        """ Send ccd wipe command and handle reply """
        cmdVar = self.actor.crudeCall(cmd, actor=self.ccd, cmdStr='wipe', timeLim=30)
        if cmdVar.didFail:
            raise exception.WipeFailed(self.ccd, interpretFailure(cmdVar))

        return dt.utcnow()

    def _read(self, cmd, visit, dateobs, exptime=None):
        """ Send ccd read command and handle reply. """
        self.time_exp_start = dateobs
        self.time_exp_end = dt.utcnow()

        darktime = round((self.time_exp_end - self.wiped).total_seconds(), 3)
        exptime = darktime if exptime is None else exptime
        exptime = round(exptime, 3)

        cmdVar = self.actor.crudeCall(cmd, actor=self.ccd, cmdStr=f'read {self.exptype}',
                                      visit=visit, exptime=exptime, darktime=darktime, obstime=dateobs.isoformat())

        if cmdVar.didFail:
            raise exception.ReadFailed(self.ccd, interpretFailure(cmdVar))

        self.readVar = cmdVar
        return exptime

    def integrate(self):
        """ Integrate for exptime in seconds, doFinish==doAbort at the beginning of integration. """
        if self.exp.doFinish:
            self.exp.failures.add("ExposureAborted(doFinish requested before exposing)")
            raise exception.ExposureAborted

        tlim = self.wiped + timedelta(seconds=self.exp.exptime)

        while dt.utcnow() < tlim:
            if self.exp.doAbort:
                raise exception.ExposureAborted
            if self.exp.doFinish:
                break

            wait()

        return self.wiped

    def clearExposure(self, cmd):
        """ Call ccdActor clearExposure command """
        if self.cleared is None:
            self.cleared = False
            self.actor.safeCall(cmd, actor=self.ccd, cmdStr='clearExposure', timeLim=2)
            self.cleared = True

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine for calib object. """
        try:
            self.wiped = self._wipe(cmd)
            dateobs = self.integrate()
            self.exptime = self._read(cmd, visit, dateobs)

        except exception.ExposureAborted:
            self.clearExposure(cmd)
        except (exception.WipeFailed, exception.ReadFailed) as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    @threaded
    def wipe(self, cmd):
        """ Wipe in thread. """
        try:
            self.wiped = self._wipe(cmd)
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

        keys = cmdVarToKeys(cmdVar=self.readVar)
        visit, beamConfigDate = keys['beamConfigDate'].values
        camStr, dateDir, visit, specNum, armNum = keys['spsFileIds'].values
        cam = self.actor.specFromNum(specNum=specNum, armNum=armNum)

        try:
            utils.insert(opdb.OpDB.url, 'sps_exposure',
                         pd.DataFrame(dict(pfs_visit_id=int(visit), sps_camera_id=cam.camId, exptime=self.exptime,
                                           time_exp_start=self.time_exp_start, time_exp_end=self.time_exp_end,
                                           beam_config_date=float(beamConfigDate)), index=[0]))
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
