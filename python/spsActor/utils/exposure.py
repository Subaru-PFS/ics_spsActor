import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.opdb import opDB
from ics.utils.threading import threaded
from opscore.utility.qstr import qstr
from spsActor.utils import lampsControl
from spsActor.utils.ids import SpsIds as idsUtils


class SpecModuleExposure(QThread):
    """ Placeholder to handle spectograph module cmd threading. """
    EnuExposeTimeMargin = 5

    def __init__(self, exp, specNum, arms):
        self.exp = exp
        # have specModule config handy.
        self.specConfig = exp.actor.spsConfig.specModules[f'sm{specNum}']

        self.arms = arms
        self.enuName = f'enu_{self.specName}'

        QThread.__init__(self, exp.actor, self.specName)

        # create underlying ccd exposure objects.
        self.camExp = [CcdExposure(exp, f'{arm}{specNum}') for arm in arms]

        # add callback for shutters state, useful to fire process asynchronously.
        self.shuttersKeyVar = self.exp.actor.models[self.enuName].keyVarDict['shutters']
        self.shuttersKeyVar.addCallback(self.shuttersState)
        self.shuttersOpen = None

        # instantiate IIS control if required.
        if self.doControlIIS:
            self.iisControl = lampsControl.IISControl(self.exp, self.enuName)
        else:
            self.iisControl = None

        self.start()

    @property
    def specName(self):
        return self.specConfig.specName

    @property
    def doControlIIS(self):
        return self.exp.doIIS

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

    def shutterMask(self):
        """ Build argument to enu shutters expose cmd. """
        shutters = list(set(sum([self.specConfig.shutterSet(arm, lightBeam=True) for arm in self.arms], [])))
        bitMask = 0 if not shutters else sum([shutter.bitMask for shutter in shutters])
        return f'0x{bitMask:x}'

    def wipe(self, cmd):
        """ Wipe running CcdExposure and wait for integrating state.
        Note that doFinish==doAbort at the beginning of integration.  """
        for camExp in self.runExp:
            camExp.wipe(cmd)

        while not any(self.currently(state='wiping')):
            pfsTime.sleep.millisec()

        while not all(self.currently(state='integrating')):
            pfsTime.sleep.millisec()

        if self.exp.doFinish:
            raise exception.EarlyFinish

        if self.exp.doAbort or any(self.clearedExp):
            raise exception.ExposureAborted

    def integrate(self, cmd, shutterTime=None):
        """ Integrate for both calib and regular exposure. """
        # exposure time can have some overhead.
        shutterTime = self.exp.exptime if shutterTime is None else shutterTime

        shutterMask = self.shutterMask()
        cmdVar = self.exp.actor.crudeCall(cmd, actor=self.enuName,
                                          cmdStr=f'shutters expose exptime={shutterTime} shutterMask={shutterMask}',
                                          timeLim=shutterTime + SpecModuleExposure.EnuExposeTimeMargin)
        if self.exp.doAbort:
            raise exception.ExposureAborted

        if cmdVar.didFail:
            raise exception.ShuttersFailed(self.specName, cmdUtils.interpretFailure(cmdVar))

        keys = cmdUtils.cmdVarToKeys(cmdVar)

        exptime = float(keys['exptime'].values[0])
        dateobs = pfsTime.Time.fromisoformat(keys['dateobs'].values[0]).isoformat()
        # dateobs = keys['dateobs'].values[0]

        return exptime, dateobs

    def read(self, cmd, visit, exptime, dateobs):
        """ Read running CcdExposure and wait for idle state. """
        for camExp in self.runExp:
            camExp.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        while not all(self.currently(state='idle')):
            pfsTime.sleep.millisec()

    @threaded
    def expose(self, cmd, visit):
        """ Full exposure routine, exceptions are catched and handled under the cover. """

        try:
            self.wipe(cmd)
            exptime, dateobs = self.integrate(cmd)
            self.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        except Exception as e:
            self.clearExposure(cmd)
            self.exp.abort(cmd, reason=str(e))

    def shuttersState(self, keyVar):
        """ Shutters state callback, call shuttersOpenCB() whenever open. """
        state = keyVar.getValue(doRaise=False)

        # track shutters state.
        self.actor.bcast.debug(f'text="{self.specName} shutters {state}"')

        # should cover all cases.
        self.shuttersOpen = 'open' in state

        if self.shuttersOpen:
            self.shuttersOpenCB()

    def shuttersOpenCB(self):
        """ callback called whenenever shutters are opened. """
        # fire IIS is required.
        if self.doControlIIS:
            self.iisControl.goSignal = True

    def clearExposure(self, cmd):
        """ Clear all running CcdExposure. """
        for camExp in self.runExp:
            camExp.clearExposure(cmd)

    def abort(self, cmd):
        """ Command shutters to abort exposure. """
        if any(self.currently(state='integrating')):
            self.exp.actor.safeCall(cmd, actor=self.enuName, cmdStr='exposure finish')

    def finish(self, cmd):
        """ Command shutters to finish exposure. """
        if any(self.currently(state='integrating')):
            self.exp.actor.safeCall(cmd, actor=self.enuName, cmdStr='exposure finish')

    def exit(self):
        """ Free up all resources. """
        # remove shutters callback.
        self.shuttersKeyVar.removeCallback(self.shuttersState)

        for camExp in self.camExp:
            camExp.exit()

        self.camExp.clear()
        QThread.exit(self)


class Exposure(object):
    """ Exposure object. """
    SpecModuleExposureClass = SpecModuleExposure

    def __init__(self, actor, exptype, exptime, cams, doIIS=False, doTest=False, window=False):
        # force exptype == test.
        exptype = 'test' if doTest else exptype

        self.doAbort = False
        self.doFinish = False
        self.actor = actor
        self.exptype = exptype
        self.exptime = exptime

        # IIS flag.
        self.doIIS = doIIS
        # Define how ccds are wiped and read.
        self.wipeFlavour, self.readFlavour = self.defineCCDControl(window)

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
    def iisThreads(self):
        return list(filter(None, [smThread.iisControl for smThread in self.smThreads]))

    @property
    def lampsThreads(self):
        return self.iisThreads

    @property
    def threads(self):
        return self.smThreads + self.lampsThreads

    def defineCCDControl(self, windows):
        """ Declare kind of ccd wipe and ccd reads.  """
        if windows:
            row0, nrows = windows
            wipeFlavour = 'nrows=0'
            readFlavour = f'row0={row0} nrows={nrows}'
        else:
            wipeFlavour = ''
            readFlavour = ''

        return wipeFlavour, readFlavour

    def instantiate(self, cams):
        """ Create underlying specModuleExposure threads.  """
        return [self.SpecModuleExposureClass(self, smId, arms) for smId, arms in idsUtils.camToArmDict(cams).items()]

    def waitForCompletion(self, cmd, visit):
        """ Create underlying specModuleExposure threads.  """

        def genFileIds(visit, frames):
            return f"""fileIds={visit},{qstr(';'.join(frames))},0x{idsUtils.getMask(frames):04x}"""

        self.start(cmd, visit)

        while not self.isFinished:
            pfsTime.sleep.millisec()

        if self.storable:
            frames = self.store(cmd, visit)
            return genFileIds(visit, frames)

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
        # start lamp thread if any.
        for thread in self.lampsThreads:
            thread.start(cmd)

        for thread in self.smThreads:
            thread.expose(cmd, visit)

    def exit(self):
        """ Free up all resources. """
        for thread in self.threads:
            thread.exit()

        self.smThreads.clear()

    def store(self, cmd, visit):
        """Store Exposure in sps_visit table in opdb database. """
        try:
            opDB.insert('sps_visit', pfs_visit_id=visit, exp_type=self.exptype)
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
        return self.smThreads

    @property
    def lampsThreads(self):
        return []

    def instantiate(self, cams):
        """ Create underlying CcdExposure threads object. """
        return [CcdExposure(self, cam) for cam in cams]


class CcdExposure(QThread):
    # timeout setting.
    wipeTimeLim = 30
    readTimeLim = 90
    clearTimeLim = 10
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
        cmdVar = self.actor.crudeCall(cmd, actor=self.ccd, cmdStr=f'wipe {self.exp.wipeFlavour}',
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
                                                                  f'{self.exp.readFlavour}',
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
