import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
from actorcore.QThread import QThread
from ics.utils.opdb import opDB
from ics.utils.threading import threaded
from opscore.utility.qstr import qstr
from spsActor.utils import ccdExposure
from spsActor.utils import hxExposure
from spsActor.utils import lampsControl
from spsActor.utils.ids import SpsIds as idsUtils


def factory(exp, cam):
    """Return Exposure object given the cam"""

    if cam.arm in ['b', 'r', 'm']:
        return ccdExposure.CcdExposure(exp, cam)
    elif cam.arm in ['n']:
        return hxExposure.HxExposure(exp, cam)
    else:
        raise ValueError(f'unknown arm:{cam.arm} ..')


class SpecModuleExposure(QThread):
    """ Placeholder to handle spectograph module cmd threading. """
    EnuExposeTimeMargin = 5

    def __init__(self, exp, specNum, cams):
        self.exp = exp
        # have specModule config handy.
        self.specConfig = exp.actor.spsConfig.specModules[f'sm{specNum}']

        self.cams = cams
        self.enuName = f'enu_{self.specName}'

        QThread.__init__(self, exp.actor, self.specName)

        # create underlying exposure objects.
        self.camExp = [factory(exp, cam) for cam in cams]

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
    def arms(self):
        return [cam.arm for cam in self.cams]

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

        while not all([ccd.wiped for ccd in self.runExp]):
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

    def __init__(self, actor, visit, exptype, exptime, cams, doIIS=False, doTest=False, blueWindow=False, redWindow=False):
        # force exptype == test.
        exptype = 'test' if doTest else exptype

        self.doAbort = False
        self.doFinish = False
        self.actor = actor
        self.visit = visit
        self.exptype = exptype
        self.exptime = exptime

        # Define how ccds are wiped and read.
        self.wipeFlavour = dict(b='', r='')
        self.readFlavour = dict(b='', r='')

        # IIS flag.
        self.doIIS = doIIS
        # update ccd control if windowing is activated
        self.defineCCDControl(blueWindow, redWindow)

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

    def defineCCDControl(self, blueWindow, redWindow):
        """Update CCD wipe and read flavours based on windowing."""
        for arm, window in zip('br', [blueWindow, redWindow]):
            if not window:
                continue

            row0, nrows = window
            self.wipeFlavour[arm] = 'nrows=0'
            self.readFlavour[arm] = f'row0={row0} nrows={nrows}'

    def instantiate(self, cams):
        """ Create underlying specModuleExposure threads.  """
        return [self.SpecModuleExposureClass(self, smId, cams) for smId, cams in idsUtils.splitCamPerSpec(cams).items()]

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
        return [ccdExposure.CcdExposure(self, cam) for cam in cams]
