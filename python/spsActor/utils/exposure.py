import ics.utils.cmd as cmdUtils
import ics.utils.time as pfsTime
import spsActor.utils.exception as exception
import spsActor.utils.metadata as metadata
from actorcore.QThread import QThread
from ics.utils.opdb import opDB
from ics.utils.threading import singleShot
from ics.utils.threading import threaded
from opscore.utility.qstr import qstr
from spsActor.utils import ccdExposure
from spsActor.utils import hxExposure
from spsActor.utils import iisControl
from spsActor.utils import lampsControl
from spsActor.utils import shutters
from spsActor.utils.ids import SpsIds as idsUtils
from twisted.internet import reactor


def factory(exp, cam):
    """Return Exposure object given the cam"""
    if cam.arm in 'brm':
        return ccdExposure.CcdExposure(exp, cam)
    elif cam.arm in 'n':
        return hxExposure.HxExposure(exp, cam)
    else:
        raise ValueError(f'unknown arm:{cam.arm} ..')


class SpecModuleExposure(QThread):
    """Placeholder to handle spectograph module cmd threading."""
    EnuExposeTimeMargin = 5

    def __init__(self, exp, specNum, cams):
        self.exp = exp
        # have specModule config handy.
        self.specConfig = exp.actor.spsConfig[f'sm{specNum}']

        self.cams = cams
        self.enuName = f'enu_{self.specName}'
        self.enuKeyVarDict = self.exp.actor.models[self.enuName].keyVarDict

        QThread.__init__(self, exp.actor, self.specName)

        # create underlying exposure objects.
        self.camExp = [factory(exp, cam) for cam in cams]

        # creating shutter state object.
        self.shutterState = shutters.ShutterState(self)
        self.enuKeyVarDict['shutters'].addCallback(self.shutterState.callback)

        # instantiate IIS control if required.
        if self.doControlIIS:
            self.iisControl = iisControl.IISControl(self.exp, self.enuName)
        else:
            self.iisControl = None

        self.start()

    @property
    def specName(self):
        return self.specConfig.specName

    @property
    def specNum(self):
        return self.specConfig.specNum

    @property
    def lightSource(self):
        return self.specConfig.lightSource

    @property
    def arms(self):
        return [cam.arm for cam in self.cams]

    @property
    def hxExposure(self):
        for camExp in self.camExp:
            if isinstance(camExp, hxExposure.HxExposure):
                return camExp

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
        return all([camExp.isFinished for camExp in self.camExp])

    @property
    def syncThreadsToOpen(self):
        # if not syncSpectrograph, each sm is independent.
        return self.exp.runExp if self.exp.syncSpectrograph else self.runExp

    def currently(self, state):
        """Current camExp states."""
        return [camExp.state == state for camExp in self.runExp]

    def shutterMask(self):
        """Build argument to enu shutters expose cmd."""
        shutters = list(set(sum([self.specConfig.shutterSet(arm, lightBeam=True) for arm in self.arms], [])))
        bitMask = 0 if not shutters else sum([shutter.bitMask for shutter in shutters])
        return f'0x{bitMask:x}'

    def wipe(self, cmd):
        """Wipe running CcdExposure and wait for integrating state.
        Note that doFinish==doAbort at the beginning of integration."""

        def checkAbortSignal():
            if self.exp.doFinish:
                raise exception.EarlyFinish

            if self.exp.doAbort:
                raise exception.ExposureAborted

        # Start the ramp.
        if self.hxExposure:
            self.hxExposure.ramp(cmd, expectedExptime=self.exp.exptime)

            # And wait for the reset frame to start wiping ccds.
            while self.hxExposure.state != 'reset':
                self.hxExposure.checkResetTiming()  # check that that reset is done in timely manner.
                checkAbortSignal()
                pfsTime.sleep.millisec()

        for camExp in self.runExp:
            if camExp == self.hxExposure:
                continue
            camExp.wipe(cmd)

        # # if one fails, it cleared itself out.
        while not all([detector.wiped for detector in self.syncThreadsToOpen]):
            if self.hxExposure:
                self.hxExposure.checkFirstReadTiming()  # check that the first read is reached in timely manner.
            pfsTime.sleep.millisec()

        checkAbortSignal()

    def integrate(self, cmd, shutterTime=None):
        """Integrate for both calib and regular exposure."""
        # exposure time can have some overhead.
        shutterTime = self.exp.exptime if shutterTime is None else shutterTime

        shutterMask = self.shutterMask()
        cmdVar = self.exp.actor.crudeCall(cmd, actor=self.enuName,
                                          cmdStr=f'shutters expose exptime={shutterTime} shutterMask={shutterMask} visit={self.exp.visit}',
                                          timeLim=shutterTime + SpecModuleExposure.EnuExposeTimeMargin)

        if cmdVar.didFail:
            raise exception.ShuttersFailed(self.specName, cmdUtils.interpretFailure(cmdVar))

        keys = cmdUtils.cmdVarToKeys(cmdVar)

        exptime = float(keys['exptime'].values[0])
        dateobs = pfsTime.Time.fromisoformat(keys['dateobs'].values[0]).isoformat()
        # dateobs = keys['dateobs'].values[0]

        return exptime, dateobs

    def read(self, cmd, visit, exptime, dateobs):
        """Read running CcdExposure and wait for idle state."""
        for camExp in self.runExp:
            camExp.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

        while not all(self.currently(state='idle')):
            pfsTime.sleep.millisec()

    @threaded
    def expose(self, cmd, visit):
        """Full exposure routine, exceptions are catched and handled under the cover."""

        try:
            self.wipe(cmd)
            self.postWipeFunc()
            exposeStart = pfsTime.Time.now()
            try:
                exptime, dateobs = self.integrate(cmd)
            except Exception as e:
                if not self.shutterState.wasOpen:
                    self.actor.logger.warning(f'{self.specName} shutters failed before opening, discarding data...')
                    pfsTime.sleep.millisec(1000)
                    raise

                self.actor.logger.warning(f'{self.specName} shutters failed after opening, still reading data...')
                self.exp.failures.add(reason=str(e))
                exptime = pfsTime.Time.now().timestamp() - exposeStart.timestamp()
                dateobs = exposeStart.isoformat()

        except Exception as e:
            self.exp.abort(cmd, reason=str(e))
            return

        self.read(cmd, visit=visit, exptime=exptime, dateobs=dateobs)

    def shuttersOpenCB(self):
        """Callback called whenenever shutters are opened."""
        self.exp.genShutterKey('open', lightSource=self.specConfig.lightSource)

        # fire IIS is required.
        if self.doControlIIS:
            self.iisControl.goSignal = True

    def shuttersCloseCB(self):
        """Callback called whenenever shutters are closed after the exposure."""
        self.exp.genShutterKey('close', lightSource=self.specConfig.lightSource)

        # Declare final read, that will call finishRamp on the next hxRead callback.
        if self.hxExposure:
            self.hxExposure.declareFinalRead()

    def iisIlluminated(self):
        """Check if iis was illuminated during that visit."""
        # enuKeyVarDict and lampKeyVarDict are the same for IIS.
        return iisControl.IISControl.checkIllumination(self.exp.visit, self.enuKeyVarDict, self.enuKeyVarDict)

    def illuminated(self):
        """Check if science fibers are illuminated."""
        # consider illuminated by default.
        illuminated = True

        if self.exp.exptype in ['arc', 'flat']:
            # DCB model can be trusted I think.
            if self.lightSource.useDcbActor:
                lampKeyVarDict = self.actor.models[self.lightSource.lampsActor].keyVarDict
                return lampsControl.LampsControl.checkIllumination(self.exp.visit, self.enuKeyVarDict, lampKeyVarDict)
            elif self.lightSource == 'pfi':
                # illuminated = False // this caused some confusion in december 2023 run (hgcd...)
                #  I think considering illuminated = True for pfi is the right answer.
                pass

        return illuminated

    def clearExposure(self, cmd):
        """Clear all running CcdExposure."""
        for camExp in self.runExp:
            camExp.clearExposure(cmd)

    def abort(self, cmd):
        """discarding exposure."""
        self.finish(cmd, doDiscard=True)

    @singleShot
    def finish(self, cmd, doDiscard=False):
        """
        Command shutters to finish the exposure, using singleShot so the operation runs in parallel.

        Parameters:
        ----------
        cmd : object
            The command object used to send and manage the shutter control.
        doDiscard : bool, optional
            If True, the exposure will be discarded (CCDs cleared and ramp stopped), regardless of shutter state.
            Default is False.

        Notes:
        ------
        - If the shutters were not open or if doDiscard is True, the exposure is discarded by clearing the CCDs
          and stopping the ramp.
        - If the exposure was discarded, when the shutter command returns, the read command will be skipped.
        - The finish operation only completes when the shutters are fully closed.
        """
        # If shutters were not open or doDiscard is forced, discard CCDs and stop the ramp.
        if not self.shutterState.wasOpen or doDiscard:
            self.clearExposure(cmd)

        if self.shutterState.isOpen:
            # It the exposure was discarded, when the shutter command returns, the read command will be skipped.
            # The finish operation only completes when the shutters are closed.
            self.exp.actor.safeCall(cmd, actor=self.enuName, cmdStr='exposure finish')

    def postWipeFunc(self):
        """Placeholder for a function call after wipe."""
        pass

    def exit(self):
        """Free up all resources."""
        # remove shutters callback.
        self.enuKeyVarDict['shutters'].removeCallback(self.shutterState.callback)

        for camExp in self.camExp:
            camExp.exit()

        self.camExp.clear()
        QThread.exit(self)


class Exposure(object):
    """Exposure object."""
    SpecModuleExposureClass = SpecModuleExposure

    def __init__(self, actor, visit, exptype, exptime, cams, metadata=None, doIIS=False, doTest=False, blueWindow=False,
                 redWindow=False, expTimeOverHead=0, **kwargs):
        self.actor = actor
        self.visit = visit
        self.coreExpType = exptype  # save the actual exptype first
        self.exptype = 'test' if doTest else exptype  # force exptype == test if doTest
        self.exptime = exptime
        self.metadata = metadata
        self.doIIS = doIIS

        # Define how ccds are wiped and read, for windowing purposes.
        self.wipeFlavour, self.readFlavour = ccdExposure.CcdExposure.defineCCDControl(blueWindow, redWindow)

        self.syncSpectrograph = self.exposureConfig['doSyncSpectrograph']
        self.expTimeOverHead = max(self.exposureConfig['expTimeOverHead'], expTimeOverHead)
        self.rampConfig = self.exposureConfig['ramp']



        self.cmd = None
        self.doAbort = False
        self.doFinish = False
        self.didGenShutterKey = dict(open=False, close=False)

        self.failures = exception.Failures()
        self.smThreads = self.instantiate(cams)

    @property
    def exposureConfig(self):
        return self.actor.actorConfig['exposure']

    @property
    def camExp(self):
        return sum([th.camExp for th in self.smThreads], [])

    @property
    def clearedExp(self):
        return [camExp for camExp in self.camExp if camExp.cleared]

    @property
    def runExp(self):
        return list(set(self.camExp) - set(self.clearedExp))

    @property
    def isFinished(self):
        return all([th.isFinished for th in self.smThreads])

    @property
    def storable(self):
        return any([camExp.storable for camExp in self.camExp])

    @property
    def iisThreads(self):
        return list(filter(None, [smThread.iisControl for smThread in self.smThreads]))

    @property
    def lampsThreads(self):
        return self.iisThreads

    @property
    def threads(self):
        return self.smThreads + self.lampsThreads

    def instantiate(self, cams):
        """Create underlying specModuleExposure threads."""
        return [self.SpecModuleExposureClass(self, smId, cams) for smId, cams in idsUtils.splitCamPerSpec(cams).items()]

    def waitForCompletion(self, cmd, visit):
        """Create underlying specModuleExposure threads."""

        def genFileIds(visit, frames):
            return f"""fileIds={visit},{qstr(';'.join(frames))},0x{idsUtils.getMask(frames):04x}"""

        self.start(cmd, visit)

        while not self.isFinished:
            pfsTime.sleep.millisec()

        if self.storable:
            frames = self.store(cmd, visit)
        else:
            frames = []

        return genFileIds(visit, frames)

    def abort(self, cmd, reason="ExposureAborted()"):
        """ Abort current exposure."""
        # just call finish.
        self.doAbort = True
        self.failures.add(reason)

        for thread in self.threads:
            thread.abort(cmd)

    def finish(self, cmd):
        """Finish current exposure."""
        self.doFinish = True

        for thread in self.threads:
            thread.finish(cmd)

    def start(self, cmd, visit):
        """Start all spectrograph module exposures."""
        # just convenient.
        if not self.cmd:
            self.cmd = cmd

        # start lamp thread if any.
        for thread in self.lampsThreads:
            thread.start(cmd)

        for thread in self.smThreads:
            thread.expose(cmd, visit)

    def genShutterKey(self, state, lightSource):
        """Generate a keyword for Gen2, declaring when any PFI-connected shutter becomes open,
        and when all PFI-connected shutters become closed."""
        doGenerate = not self.didGenShutterKey[state]

        if state == 'close' and not all([specModule.shutterState.didExpose for specModule in self.smThreads]):
            doGenerate = False

        if doGenerate:
            self.didGenShutterKey[state] = True

            if lightSource == 'pfi':
                self.cmd.inform(f'pfiShutters={state}')

            # Generate fiberIllumination keyword, e.g. was IIS used etc...
            if state == 'close':
                reactor.callLater(1, self.genIlluminationStatus)

    def genIlluminationStatus(self):
        """Generate fiberIllumination keyword using a single unsigned integer."""

        fiberIllumination = 0  # Initialize an 8-bit integer (all bits set to 0).

        for iSpec in range(4):
            specNum = iSpec + 1
            specModule = [thread for thread in self.smThreads if thread.specNum == specNum]

            # No data associated with this spectrograph module.
            if not specModule:
                continue  # Keep the 2 bits as 00 (already 0 by default).

            [specModule] = specModule  # Unpack the single item from the list.

            # Default state: both bits set (11 in binary).
            status = 3  # Both engineering and science fibers illuminated (11 in binary).

            # Checking engineering fibers illumination.
            if not specModule.iisIlluminated():
                status &= ~1  # Clear the least significant bit.

            # Checking science fibers illumination.
            if not specModule.illuminated():
                status &= ~2  # Clear the second bit.

            # Shift the status by (iSpec * 2) bits and OR it into the fiberIllumination integer.
            fiberIllumination |= (status << (iSpec * 2))

        # Format the 8-bit integer as a binary string for display.
        self.cmd.inform(f'fiberIllumination={self.visit},0x{fiberIllumination:02x}')

    def parsePfsDesign(self):
        if not self.metadata:
            return False

        designId, designName, visit0, sequenceId, groupId = self.metadata
        return f'0x{designId:016x},{qstr(designName)}'

    def parseMetadata(self):
        if not self.metadata:
            return False

        designId, designName, visit0, sequenceId, groupId = self.metadata
        return ','.join(map(str, [visit0, sequenceId, groupId]))


    def exit(self):
        """Free up all resources."""
        for thread in self.threads:
            thread.exit()

        self.smThreads.clear()

    def store(self, cmd, visit):
        """Store Exposure in sps_visit table in opdb database."""
        try:
            opDB.insert('sps_visit', pfs_visit_id=visit, exp_type=self.exptype)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))

        frames = [camExp.store() for camExp in self.camExp]
        return list(filter(None, frames))


class DarkExposure(Exposure):
    """DarkExposure object."""

    def __init__(self, *args, **kwargs):
        Exposure.__init__(self, *args, **kwargs)

    @property
    def camExp(self):
        return self.smThreads

    @property
    def lampsThreads(self):
        return []

    def instantiate(self, cams):
        """Create underlying CcdExposure threads object."""
        return [factory(self, cam) for cam in cams]
