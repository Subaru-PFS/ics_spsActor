from datetime import datetime as dt
from datetime import timedelta

from actorcore.QThread import QThread
from pfs.utils.opdb import opDB
from pfs.utils.spectroIds import SpectroIds
from spsActor.utils import cmdKeys, camPerSpec, wait, threaded, fromisoformat


class Exposure(object):
    """ Exposure object. """

    def __init__(self, actor, exptype, exptime, cams, doLamps=False):
        self.doAbort = False
        self.doFinish = False
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
            thread.expose(cmd, visit)

    def exit(self):
        """ Free up all resources. """
        for thread in self.threads:
            thread.exit()

        self.threads.clear()

    def store(self, cmd, visit):
        """Store Exposure in sps_visit table in opdb database. """
        try:
            opDB.insert('sps_visit', pfs_visit_id=visit, exp_type=self.exptype)
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
        self.enu = f'enu_sm{smId}'
        self.camExp = [CcdExposure(exp, f'{arm}{smId}') for arm in arms]

        QThread.__init__(self, exp.actor, f'sm{smId}')
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
        """ Wipe running CcdExposure and wait for integrating state. """
        for camExp in self.runExp:
            camExp.wipe(cmd)

        while not all(self.currently(state='wiping')):
            wait()

        while not all(self.currently(state='integrating')):
            wait()

        if not self.runExp or self.exp.doAbort:
            raise RuntimeError

    def integrate(self, cmd, doLamps=False):
        """ Integrate for both calib and regular exposure """

        if doLamps:
            shutterTime = self.exp.exptime + 4
            lampq = self.actor.cmdr.cmdq(actor='dcb',
                                         cmdStr=f'go delay=2',
                                         timeLim=shutterTime+10,
                                         forUserCmd=cmd)
        else:
            shutterTime = self.exp.exptime
            lampq = None

        shutters = self.getShutters()
        cmdVar = self.exp.actor.safeCall(cmd, actor=self.enu, timeLim=shutterTime + 30,
                                         cmdStr=f'shutters expose {shutters}', exptime=shutterTime)

        if cmdVar.didFail:
            raise RuntimeError('failed to control shutters!')
        keys = cmdKeys(cmdVar)

        if doLamps:
            lampCmdVar = lampq.get()
            if lampCmdVar.didFail():
                raise RuntimeError(f'failed to control lamps: {lampsCmdVar}')
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

    @threaded
    def expose(self, cmd, visit, doWipe=True, doLamps=False):
        """ Full exposure routine, exceptions are catched and handled under the cover. """

        try:
            if doWipe:
                self.wipe(cmd)
            exptime, dateobs = self.integrate(cmd, doLamps=doLamps)
            self.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        except RuntimeError:
            self.clear(cmd)

    def clear(self, cmd):
        """ Clear all running CcdExposure. """
        for camExp in self.runExp:
            camExp.clear(cmd)

    def abort(self, cmd):
        """ Command shutters to abort exposure. """
        if any(self.currently(state='integrating')):
            self.exp.actor.safeCall(cmd, actor=self.enu, cmdStr='exposure abort')

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
            raise RuntimeError

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
            raise RuntimeError

        self.readVar = cmdVar
        return exptime

    def integrate(self):
        """ Integrate for exptime in seconds """
        if self.exp.doAbort:
            raise RuntimeError

        tlim = self.wiped + timedelta(seconds=self.exp.exptime)

        while dt.utcnow() < tlim:
            if self.exp.doAbort:
                raise RuntimeError
            if self.exp.doFinish:
                break

            wait()

        return self.wiped

    def clear(self, cmd):
        """ Call ccdActor clearExposure command """
        self.actor.safeCall(cmd, actor=self.ccd, cmdStr='clearExposure')
        self.cleared = True

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine for calib object. """
        try:
            self.wiped = self._wipe(cmd)
            dateobs = self.integrate()
            self.exptime = self._read(cmd, visit, dateobs)

        except RuntimeError:
            self.clear(cmd)

    @threaded
    def wipe(self, cmd):
        """ Wipe in thread. """
        try:
            self.wiped = self._wipe(cmd)
        except RuntimeError:
            self.clear(cmd)

    @threaded
    def read(self, cmd, visit, dateobs, exptime):
        """ Read in thread. """
        try:
            self.exptime = self._read(cmd, visit, dateobs, exptime)
        except RuntimeError:
            self.clear(cmd)

    def store(self):
        """ Store in sps_exposure in opDB database. """
        if not self.storable:
            return

        keys = cmdKeys(cmdVar=self.readVar)
        camStr, dateDir, visit, specNum, armNum = keys['spsFileIds'].values
        cam = self.actor.specFromNum(specNum=specNum, armNum=armNum)

        try:
            opDB.insert('sps_exposure', pfs_visit_id=int(visit), sps_camera_id=cam.camId, exptime=self.exptime,
                        time_exp_start=self.time_exp_start, time_exp_end=self.time_exp_end)
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
