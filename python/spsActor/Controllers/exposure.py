from datetime import datetime as dt
from datetime import timedelta

import numpy as np
from actorcore.QThread import QThread
from pfs.utils.opdb import opDB
from spsActor.utils import cmdKeys, camPerSpec, wait, threaded, describe


class Exposure(object):
    """ Exposure object """

    def __init__(self, actor, exptype, exptime, cams):
        self.doAbort = False
        self.doFinish = False
        self.actor = actor
        self.exptype = exptype
        self.exptime = exptime
        self.obsdate = dt.utcnow()

        self.smExp = [SmExposure(self, smId, cams) for smId, cams in camPerSpec(cams).items()]

    @property
    def camExp(self):
        return sum([smExp.camExp for smExp in self.smExp], [])

    @property
    def notFinished(self):
        return False in [smExp.isFinished for smExp in self.smExp]

    @property
    def isIdle(self):
        return 'idle' in [camExp.state for camExp in self.camExp]

    def abort(self, cmd):
        self.doAbort = True

    def finish(self, cmd):
        self.doFinish = True

    def start(self, cmd, visit):
        """ Start all spectrograph module exposures """
        for exp in self.smExp:
            exp.expose(cmd, visit)

    def exit(self):
        """ Free up all resources """
        for smExp in self.smExp:
            smExp.exit()
        delattr(self, 'smExp')

    def store(self, cmd, visit):
        """Store Exposure in sps_visit table in opdb database """
        try:
            opDB.insert('sps_visit', pfs_visit_id=visit, exp_type=self.exptype)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))


class SmExposure(QThread):
    """ Placeholder to handle spectograph module cmd threading """

    def __init__(self, exp, smId, arms):
        self.exp = exp
        self.exptype = exp.exptype
        self.exptime = exp.exptime
        self.smId = smId
        self.arms = arms
        self.enu = f'enu_sm{smId}'
        self.camExp = [CamExposure(self, arm) for arm in arms]
        self.isFinished = False

        QThread.__init__(self, exp.actor, f'sm{smId}')
        self.start()

    @property
    def runExp(self):
        return [camExp for camExp in self.camExp if not camExp.cleared]

    @property
    def replies(self):
        """check that CamExposure(s) are finished """
        return None not in [camExp.cmdVar for camExp in self.runExp]

    def getShutters(self):
        """ Build argument to enu shutters expose cmd """
        if self.exptype in ['bias', 'dark']:
            return None
        return '' if 'b' in self.arms else 'red'

    def wipe(self, cmd):
        """ Wipe running CamExposure """
        for camExp in self.runExp:
            camExp.cmdVar = None
            camExp.wipe(cmd=cmd)

    def integrate(self, cmd):
        """ Integrate for both calib and regular exposure """
        exptime, dateobs = None, None

        if not self.exp.doAbort:
            shutters = self.getShutters()
            if shutters is not None:
                cmdVar = self.exp.actor.safeCall(actor=self.enu,
                                                 cmdStr=f'shutters expose exptime={self.exptime} {shutters}',
                                                 forUserCmd=cmd, timeLim=self.exptime + 30)

                keys = cmdKeys(cmdVar)
                exptime = float(keys['exptime'].values[0])
                if np.isnan(exptime):
                    exptime = None
                else:
                    dateobs = dt.fromisoformat(keys['dateobs'].values[0])

            else:
                exptime = self.exptime

        return exptime, dateobs

    def read(self, cmd, visit, exptime, dateobs):
        """ Read running CamExposure """
        for camExp in self.runExp:
            camExp.cmdVar = None
            camExp.read(cmd=cmd, visit=visit, exptime=exptime, dateobs=dateobs)

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine
        exceptions are catched and handled under the cover
        """
        try:
            self.wipe(cmd=cmd)
            self.waitAndHandle(state='integrating')

            exptime, dateobs = self.integrate(cmd=cmd)

            self.read(cmd=cmd, visit=visit, exptime=exptime, dateobs=dateobs)
            self.waitAndHandle(state='idle')

        except:
            pass

        finally:
            self.isFinished = True

    def waitAndHandle(self, state):
        """ Wait for CamExposure to return cmdVar """
        while not self.replies:
            wait()

        states = [camExp.state for camExp in self.camExp]

        if state not in states and not self.exp.doAbort:
            raise RuntimeError

    def exit(self):
        """ Free up all resources """
        for camExp in self.camExp:
            camExp.exit()

        delattr(self, 'camExp')
        QThread.exit(self)


class CamExposure(QThread):
    """ Placeholder to handle ccdActor cmd threading """

    def __init__(self, smExp, arm):

        self.exp = smExp.exp
        self.exptype = smExp.exptype

        self.smId = smExp.smId
        self.ccd = f'ccd_{arm}{self.smId}'

        self.cmdVar = None
        self.cleared = False
        self.storable = False

        QThread.__init__(self, self.exp.actor, self.ccd)
        QThread.start(self)

    @property
    def state(self):
        if self.cleared:
            return 'cleared'
        else:
            return self.exp.actor.models[self.ccd].keyVarDict['exposureState'].getValue(doRaise=False)

    @threaded
    def wipe(self, cmd):
        """ Send ccd wipe command and handle reply """
        cmdVar = self.actor.safeCall(actor=self.ccd, cmdStr='wipe', forUserCmd=cmd, timeLim=20)

        if self.handleReply(cmd, cmdVar=cmdVar):
            self.darktime = dt.utcnow()

    @threaded
    def read(self, cmd, visit, exptime, dateobs):
        """ Send ccd read command and handle reply
        wait for integration if calibExposure, store CamExposure
        """
        if dateobs is None and exptime is not None:
            dateobs = self.integrate(exptime)

        if dateobs is None:
            self.clear(cmd)
            return

        self.time_exp_start = dateobs
        self.time_exp_end = dt.utcnow()
        self.exptime = exptime

        darktime = (self.time_exp_end - self.darktime).total_seconds()

        cmdVar = self.actor.safeCall(actor=self.ccd,
                                     cmdStr=f'read {self.exptype} visit={visit} exptime={exptime} darktime={darktime} '
                                            f'obstime={dateobs.isoformat()}',
                                     forUserCmd=cmd,
                                     timeLim=60)

        self.storable = self.handleReply(cmd, cmdVar=cmdVar)

    def handleReply(self, cmd, cmdVar):
        """ Clear ccd is command has failed """
        if cmdVar.didFail:
            self.clear(cmd)
        else:
            self.cmdVar = cmdVar

        return not cmdVar.didFail

    def clear(self, cmd):
        """ Call ccdActor clearExposure command """
        self.cmdVar = self.actor.safeCall(actor=self.ccd, cmdStr='clearExposure', forUserCmd=cmd, timeLim=20)
        self.cleared = True

    def integrate(self, exptime):
        """ Integrate for exptime in seconds """
        tlim = self.darktime + timedelta(seconds=exptime)

        while dt.utcnow() < tlim:
            if self.exp.doAbort:
                return None
            if self.exp.doFinish:
                break

            wait()

        return self.darktime

    def store(self, cmdVar=None):
        """ Store in sps_exposure in opDB database """
        cmdVar = self.cmdVar if cmdVar is None else cmdVar
        keys = cmdKeys(cmdVar=cmdVar)
        rootDir, dateDir, filename = keys['filepath'].values

        visit, camera_id = describe(filename)

        try:
            opDB.insert('sps_exposure', pfs_visit_id=visit, sps_camera_id=camera_id, exptime=self.exptime,
                        time_exp_start=self.time_exp_start, time_exp_end=self.time_exp_end)
        except Exception as e:
            self.actor.bcast.warn('text=%s' % self.actor.strTraceback(e))

    def exit(self):
        """Store in opDB before exiting"""
        if self.storable:
            self.store()

        QThread.exit(self)

    def handleTimeout(self):
        """| Is called when the thread is idle
        """
        pass
