from datetime import datetime as dt
from datetime import timedelta

import numpy as np
from actorcore.QThread import QThread
from pfscore.spectroIds import getSite
from spsActor.utils import getKeyvar, smCam, wait
from spsActor.utils import threaded
from spsaitActor.utils.logbook import Logbook


class Exposure(object):
    """ Exposure object """

    def __init__(self, actor, exptype, exptime, cams):
        self.actor = actor
        self.exptype = exptype
        self.exptime = exptime
        self.obsdate = dt.utcnow()

        self.smExp = [SmExposure(self, smId, cams) for smId, cams in smCam(cams).items()]

    @property
    def camExp(self):
        return sum([smExp.camExp for smExp in self.smExp], [])

    @property
    def doStop(self):
        return self.actor.controllers['expose'].doStop

    @property
    def doFinish(self):
        return self.actor.controllers['expose'].doFinish

    @property
    def notFinished(self):
        return False in [smExp.isFinished for smExp in self.smExp]

    @property
    def isIdle(self):
        return 'idle' in [camExp.state for camExp in self.camExp]

    def start(self, cmd, visit):
        """ Start all spectrograph module exposures """
        for exp in self.smExp:
            exp.expose(cmd, visit)

    def exit(self):
        """ Free up all resources """
        for smExp in self.smExp:
            smExp.exit()
        delattr(self, 'smExp')

    def store(self, visit):
        """Store Exposure in Logbook database """
        site = getSite()
        Logbook.newExposure(exposureId=f'PF{site}A{str(visit).zfill(6)}',
                            site=site,
                            visit=visit,
                            obsdate=self.obsdate.isoformat(),
                            exptime=self.exptime,
                            exptype=self.exptype)


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

        if not self.exp.doStop:
            shutters = self.getShutters()
            if shutters is not None:
                cmdVar = self.exp.actor.safeCall(actor=self.enu,
                                                 cmdStr=f'shutters expose exptime={self.exptime} {shutters}',
                                                 forUserCmd=cmd, timeLim=self.exptime + 30)

                keyvar = getKeyvar(cmdVar)
                exptime = float(keyvar['exptime'].values[0])
                if np.isnan(exptime):
                    exptime = None
                else:
                    dateobs = keyvar['dateobs'].values[0]

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

        if state not in states and not self.exp.doStop:
            raise RuntimeError

    def exit(self):
        """ Free up all resources """
        for camExp in self.camExp:
            camExp.exit()

        delattr(self, 'camExp')
        QThread.exit(self)


class CamExposure(QThread):
    """ Placeholder to handle ccdActor cmd threading """

    armNum = {'1': 'b',
              '2': 'r',
              '3': 'n',
              '4': 'm'}

    def __init__(self, smExp, arm):

        self.exp = smExp.exp
        self.exptype = smExp.exptype
        self.exptime = smExp.exptime

        self.smId = smExp.smId
        self.ccd = f'ccd_{arm}{self.smId}'

        self.cmdVar = None
        self.cleared = False

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

        darktime = (dt.utcnow() - self.darktime).total_seconds()

        cmdVar = self.actor.safeCall(actor=self.ccd,
                                     cmdStr='read %s visit=%d exptime=%.3f darktime=%.3f obstime=%s' % (self.exptype,
                                                                                                        visit,
                                                                                                        exptime,
                                                                                                        darktime,
                                                                                                        dateobs),
                                     forUserCmd=cmd,
                                     timeLim=60)

        if self.handleReply(cmd, cmdVar=cmdVar):
            self.store(cmdVar=cmdVar)

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
            if self.exp.doStop:
                return None
            if self.exp.doFinish:
                break

            wait()

        return self.darktime.isoformat()

    def store(self, cmdVar):
        """ Store CamExposure in Logbook database """
        keyvar = getKeyvar(cmdVar=cmdVar)
        rootDir, dateDir, filename = keyvar['filepath'].values

        camExposureId = filename.split('.fits')[0]
        exposureId = camExposureId[:-2]
        arm = self.armNum[camExposureId[-1]]

        Logbook.newCamExposure(camExposureId=camExposureId,
                               exposureId=exposureId,
                               smId=self.smId,
                               arm=arm)

    def handleTimeout(self):
        """| Is called when the thread is idle
        """
        pass
