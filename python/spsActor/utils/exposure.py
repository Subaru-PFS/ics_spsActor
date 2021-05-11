from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
from actorcore.QThread import QThread
from opdb import utils, opdb
from pfs.utils.sps.config import SpsConfig
from spsActor.utils import cmdKeys, camPerSpec, wait, threaded, fromisoformat


class ClearExposureASAP(Exception):
    """Exception raised when exposure is just trash and needs to be cleared ASAP.

    Attributes
    ----------
    text : `str`
       Exception text.
    """

    def __init__(self, text=""):
        Exception.__init__(self, text)


class DeadExposure(Exception):
    """Exception raised when an exposure has failed and already cleared out.

    Attributes
    ----------
    text : `str`
       Exception text.
    """

    def __init__(self, text=""):
        Exception.__init__(self, text)


class Exposure(object):
    """ Exposure object. """

    def __init__(self, actor, exptype, exptime, cams, doLamps=False, doTest=False):
        exptype = 'test' if doTest else exptype
        self.doAbort = False
        self.doFinish = False
        self.spsConfig = SpsConfig.fromConfig(actor)
        self.actor = actor
        self.exptype = exptype
        self.exptime = exptime
        self.doLamps = doLamps

        self.threads = self.instantiate(cams)

    @property
    def camExp(self):
        return sum([th.camExp for th in self.threads], [])

    @property
    def isFinished(self):
        return all([th.isFinished for th in self.threads])

    @property
    def cleared(self):
        return all([camExp.state == 'cleared' for camExp in self.camExp])

    @property
    def aborted(self):
        return self.cleared and self.doAbort

    def instantiate(self, cams):
        """ Create underlying SmExposure threads.  """
        return [SmExposure(self, smId, cams) for smId, cams in camPerSpec(cams).items()]

    def abort(self, cmd):
        """ Abort current exposure. """
        self.doAbort = True
        for thread in self.threads:
            thread.abort(cmd)

    def finish(self, cmd):
        """ Finish current exposure. """
        self.doFinish = True
        for thread in self.threads:
            thread.finish(cmd)

    def start(self, cmd, visit):
        """ Start all spectrograph module exposures. """
        for thread in self.threads:
            thread.expose(cmd, visit, doLamps=self.doLamps)

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


class Calib(Exposure):
    """ Calib object. """

    def __init__(self, *args, **kwargs):
        Exposure.__init__(self, *args, **kwargs)

    @property
    def camExp(self):
        return self.threads

    def instantiate(self, cams):
        """ Create underlying CcdExposure threads object. """
        return [CcdExposure(self, cam) for cam in cams]


class SmExposure(QThread):
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
        return [camExp for camExp in self.camExp if not camExp.cleared]

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

        while not all(self.currently(state='wiping')):
            wait()

        while not all(self.currently(state='integrating')):
            wait()

        if self.exp.doAbort or self.exp.doFinish:
            self.exp.doAbort = True
            raise ClearExposureASAP('dont even need to go further...')

        if not self.runExp:
            raise DeadExposure('all exposure are dead and cleared...')

    def integrate(self, cmd, doLamps=False):
        """ Integrate for both calib and regular exposure """

        if doLamps:
            cmd.debug(f'text="adjusting exposure for lamp control... "')
            shutterTime = self.exp.exptime + 4
            lightSource = self.exp.spsConfig.specModules[self.specName].lightSource

            lampq = self.actor.cmdr.cmdq(actor=lightSource,
                                         cmdStr=f'sources go delay=2',
                                         timeLim=shutterTime + 5,
                                         forUserCmd=cmd)
        else:
            shutterTime = self.exp.exptime
            lampq = None

        shutters = self.getShutters()
        cmdVar = self.exp.actor.safeCall(cmd, actor=self.enu, timeLim=shutterTime + 30,
                                         cmdStr=f'shutters expose {shutters}', exptime=shutterTime)

        if cmdVar.didFail or self.exp.doAbort:
            raise ClearExposureASAP

        keys = cmdKeys(cmdVar)

        if doLamps:
            cmd.debug(f'text="closing out lamp control... "')
            lampsCmdVar = lampq.get()
            cmd.debug(f'text=" cmdVar={type(lampsCmdVar)},{lampsCmdVar},{lampsCmdVar.didFail} "')
            if lampsCmdVar.didFail:
                raise ClearExposureASAP(f'failed to control lamps: {lampsCmdVar}')
            exptime = self.exp.exptime
        else:
            exptime = float(keys['exptime'].values[0])
        dateobs = fromisoformat(keys['dateobs'].values[0])

        return exptime, dateobs

    def read(self, cmd, visit, exptime, dateobs):
        """ Read running CcdExposure and wait for idle state. """
        for camExp in self.runExp:
            camExp.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        while not all(self.currently(state='idle')):
            wait()

        if not self.runExp:
            raise DeadExposure

    @threaded
    def expose(self, cmd, visit, doWipe=True, doLamps=False):
        """ Full exposure routine, exceptions are catched and handled under the cover. """

        try:
            if doWipe:
                self.wipe(cmd)
            exptime, dateobs = self.integrate(cmd, doLamps=doLamps)
            self.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        except (DeadExposure, ClearExposureASAP):
            self.clearExposure(cmd)

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


class CcdExposure(QThread):
    """ Placeholder to handle ccdActor cmd threading """

    def __init__(self, exp, cam):
        self.exp = exp
        self.exptype = exp.exptype
        self.ccd = f'ccd_{cam}'

        self.readVar = None
        self.cleared = False

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
        cmdVar = self.actor.safeCall(cmd, actor=self.ccd, cmdStr='wipe')
        if cmdVar.didFail:
            raise ClearExposureASAP

        return dt.utcnow()

    def _read(self, cmd, visit, dateobs, exptime=None):
        """ Send ccd read command and handle reply. """
        self.time_exp_start = dateobs
        self.time_exp_end = dt.utcnow()

        darktime = round((self.time_exp_end - self.wiped).total_seconds(), 3)
        exptime = darktime if exptime is None else exptime
        exptime = round(exptime, 3)

        cmdVar = self.actor.safeCall(cmd, actor=self.ccd,
                                     cmdStr=f'read {self.exptype}', visit=visit, exptime=exptime, darktime=darktime,
                                     obstime=dateobs.isoformat())

        if cmdVar.didFail:
            raise ClearExposureASAP

        self.readVar = cmdVar
        return exptime

    def integrate(self):
        """ Integrate for exptime in seconds, doFinish==doAbort at the beginning of integration. """
        if self.exp.doAbort or self.exp.doFinish:
            self.exp.doAbort = True
            raise ClearExposureASAP

        tlim = self.wiped + timedelta(seconds=self.exp.exptime)

        while dt.utcnow() < tlim:
            if self.exp.doAbort:
                raise ClearExposureASAP
            if self.exp.doFinish:
                break

            wait()

        return self.wiped

    def clearExposure(self, cmd):
        """ Call ccdActor clearExposure command """
        if not self.cleared:
            self.actor.safeCall(cmd, actor=self.ccd, cmdStr='clearExposure')
            self.cleared = True

    @threaded
    def expose(self, cmd, visit, doLamps=False):
        """ Full exposure routine for calib object. """
        try:
            self.wiped = self._wipe(cmd)
            dateobs = self.integrate()
            self.exptime = self._read(cmd, visit, dateobs)

        except (DeadExposure, ClearExposureASAP):
            self.clearExposure(cmd)

    @threaded
    def wipe(self, cmd):
        """ Wipe in thread. """
        try:
            self.wiped = self._wipe(cmd)
        except ClearExposureASAP:
            self.clearExposure(cmd)

    @threaded
    def read(self, cmd, visit, dateobs, exptime):
        """ Read in thread. """
        try:
            self.exptime = self._read(cmd, visit, dateobs, exptime)
        except ClearExposureASAP:
            self.clearExposure(cmd)

    def store(self):
        """ Store in sps_exposure in opDB database. """
        if not self.storable:
            return

        keys = cmdKeys(cmdVar=self.readVar)
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
