#!/usr/bin/env python

from functools import partial
from importlib import reload

import opscore.protocols.keys as keys
import opscore.protocols.types as types
import spsActor.Commands.cmdList as sync
import spsActor.utils.driftSlitExposure.exposure as driftSlitExposure
import spsActor.utils.driftSlitExposure.lampExposure as driftSlitLampExposure
from ics.utils.threading import singleShot
from spsActor.utils import exposure, lampsExposure

reload(exposure)
reload(sync)


class ExposeCmd(object):
    expTypes = ['bias', 'dark', 'object', 'arc', 'flat', 'domeflat']

    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a le   le argument, the parsed and typed command.
        #
        spsArgs = '[<cam>] [<cams>] [<specNum>] [<specNums>] [<arm>] [<arms>]'
        expArgs = (f'[<visit>] {spsArgs} [<metadata>] [@doTest] [@doScienceCheck] [@skipBiaCheck] '
                   f'[<bckIlluminators>] [@isLast]')
        lampsArgs = '[@doLamps] [@doShutterTiming]'
        windowingArgs = '[<window>] [<blueWindow>] [<redWindow>]'
        self.exp = dict()

        self.vocab = [
            ('expose', f'object <exptime> {expArgs} [@doIIS] {windowingArgs}', self.doExposure),
            ('expose', f'flat <exptime> {expArgs} {lampsArgs} [@doIIS] [<slideSlit>] {windowingArgs}', self.doExposure),
            ('expose', f'arc <exptime> {expArgs} {lampsArgs} [@doIIS] {windowingArgs}', self.doExposure),
            ('expose', f'domeflat <exptime> {expArgs} [@doIIS] {windowingArgs}', self.doExposure),
            ('expose', f'dark <exptime> {expArgs} {windowingArgs}', self.doExposure),
            ('expose', f'bias {expArgs} {windowingArgs}', self.doExposure),

            ('checkReady', f'{spsArgs} [@doScienceCheck] [@skipBiaCheck]', self.checkReady),

            ('erase', f'[<cam>] [<cams>]', self.doErase),

            ('exposure', 'abort <visit>', self.abort),
            ('exposure', 'finish <visit>', self.finish),
            ('exposure', 'status', self.status)
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("sps_expose", (1, 1),
                                        keys.Key("exptime", types.Float(), help="The exposure time"),
                                        keys.Key("cam", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        keys.Key('specNum', types.Int() * (1,),
                                                 help='spectrograph module(s) to take exposure from'),
                                        keys.Key('specNums', types.Int() * (1,),
                                                 help='spectrograph module(s) to take exposure from'),
                                        keys.Key("arm", types.String() * (1,),
                                                 help='arm to take exposure from'),
                                        keys.Key("arms", types.String() * (1,),
                                                 help='arm to take exposure from'),
                                        keys.Key("visit", types.Int(),
                                                 help='PFS visit id'),
                                        keys.Key("window", types.Int() * (1, 2),
                                                 help='first row, total number of rows to read, br arms'),
                                        keys.Key("blueWindow", types.Int() * (1, 2),
                                                 help='first row, total number of rows to read on blue arm'),
                                        keys.Key("redWindow", types.Int() * (1, 2),
                                                 help='first row, total number of rows to read on red arm'),
                                        keys.Key('slideSlit', types.Float() * (1, 2),
                                                 help='pixels range(start, stop )'),
                                        keys.Key('bckIlluminators', types.String() * (1,),
                                                 help='illuminator(s) lit for this exposure by somebody else'),
                                        keys.Key("metadata",
                                                 types.Long(), types.String(),
                                                 types.Int(), types.Int(), types.Int(),
                                                 types.String(), types.String(), types.String(), types.String(),
                                                 help='the metadata for the visit '
                                                      '(designId, designName, '
                                                      'visit0, sequenceId, groupId,'
                                                      'groupName, sequenceType, sequenceName, sequenceComments)'),
                                        )

    def slitsNotInHome(self, cams):
        """Spectrographs of `cams` whose slit is not home, as 'smN=<position>'."""
        notInHome = []

        for specNum in sorted(set([cam.specNum for cam in cams])):
            slitPosition = self.actor.models[f'enu_sm{specNum}'].keyVarDict['slitPosition'].getValue()

            if slitPosition != 'home':
                notInHome.append(f'sm{specNum}={slitPosition}')

        return notInHome

    def biasOn(self, cams):
        """Spectrographs of `cams` whose bia is not off, as 'smN'."""
        biaOn = []

        for specNum in sorted(set([cam.specNum for cam in cams])):
            biaStatus = self.actor.models[f'enu_sm{specNum}'].keyVarDict['bia'].getValue()

            if biaStatus != 'off':
                biaOn.append(f'sm{specNum}')

        return biaOn

    @staticmethod
    def slitError(notInHome):
        return f'SlitPositionError({" ".join(notInHome)})'

    @staticmethod
    def biaError(biaOn):
        return f'Cannot proceed: BIA is ON for spectrographs {", ".join(biaOn)}. Please turn off before retrying.'

    def notReady(self, cams, doScienceCheck, doBiaCheck):
        """What stops `cams` from exposing, as messages; empty when they are ready."""
        problems = []

        if doScienceCheck:
            notInHome = self.slitsNotInHome(cams)
            if notInHome:
                problems.append(self.slitError(notInHome))

        if doBiaCheck:
            biaOn = self.biasOn(cams)
            if biaOn:
                problems.append(self.biaError(biaOn))

        return problems

    def checkReady(self, cmd):
        """Check that the cameras are ready to expose, failing with every reason they are not."""
        cmdKeys = cmd.cmd.keywords
        cams = self.actor.spsConfig.keysToCam(cmdKeys)

        problems = self.notReady(cams, doScienceCheck='doScienceCheck' in cmdKeys,
                                 doBiaCheck='skipBiaCheck' not in cmdKeys)

        if problems:
            cmd.fail(f'text="{"; ".join(problems)}"')
            return

        cmd.finish()

    def doExposure(self, cmd):
        cmdKeys = cmd.cmd.keywords
        cams = self.actor.spsConfig.keysToCam(cmdKeys)

        exptype = None
        blueWindow = redWindow = False

        for valid in ExposeCmd.expTypes:
            exptype = valid if valid in cmdKeys else exptype

        exptime = cmdKeys['exptime'].values[0] if exptype != 'bias' else 0
        visit = cmdKeys['visit'].values[0] if 'visit' in cmdKeys else self.actor.getVisit(cmd=cmd)

        metadata = cmdKeys['metadata'].values if 'metadata' in cmdKeys else None
        doLamps = 'doLamps' in cmdKeys
        doShutterTiming = 'doShutterTiming' in cmdKeys
        doIIS = 'doIIS' in cmdKeys
        doTest = 'doTest' in cmdKeys
        doScienceCheck = 'doScienceCheck' in cmdKeys
        doBiaCheck = 'skipBiaCheck' not in cmdKeys
        doSlideSlit = 'slideSlit' in cmdKeys
        slideSlitPixelRange = cmdKeys['slideSlit'].values if doSlideSlit else False
        bckIlluminators = cmdKeys['bckIlluminators'].values if 'bckIlluminators' in cmdKeys else None
        isLast = 'isLast' in cmdKeys

        if 'window' in cmdKeys:
            blueWindow = redWindow = cmdKeys['window'].values

        blueWindow = cmdKeys['blueWindow'].values if 'blueWindow' in cmdKeys else blueWindow
        redWindow = cmdKeys['redWindow'].values if 'redWindow' in cmdKeys else redWindow

        nircam = [cam for cam in cams if cam.arm == 'n']

        if len(nircam) and (blueWindow or redWindow):
            cams = set(cams) - set(nircam)
            cmd.warn('text="ignoring nir cameras for windowed exposure."')

        problems = self.notReady(cams, doScienceCheck=doScienceCheck, doBiaCheck=doBiaCheck)
        if problems:
            cmd.fail(f'text="{problems[0]}"')
            return

        self.process(cmd, visit,
                     exptype=exptype, exptime=exptime, cams=cams, doLamps=doLamps, metadata=metadata,
                     doShutterTiming=doShutterTiming, doSlideSlit=doSlideSlit, doIIS=doIIS, doTest=doTest,
                     blueWindow=blueWindow, redWindow=redWindow, slideSlitPixelRange=slideSlitPixelRange,
                     bckIlluminators=bckIlluminators, isLast=isLast)

    @singleShot
    def process(self, cmd, visit, exptype, doLamps, doShutterTiming, doSlideSlit, doIIS, **kwargs):
        """Process exposure in another thread """

        if visit in self.exp.keys():
            cmd.fail(f'text="exposure(visit={visit}) already ongoing"')
            return

        if exptype in ['bias', 'dark']:
            cls = exposure.DarkExposure
        elif doSlideSlit:
            if doLamps or doIIS:
                cls = partial(driftSlitLampExposure.Exposure, doLamps=doLamps)
            else:
                cls = driftSlitExposure.Exposure
        elif doShutterTiming:
            cls = lampsExposure.ShutterExposure
        elif doLamps:
            cls = lampsExposure.Exposure
        else:
            cls = exposure.Exposure

        exp = cls(self.actor, visit, exptype=exptype, doIIS=doIIS, **kwargs)
        self.exp[visit] = exp

        try:
            fileIds = exp.waitForCompletion(cmd, visit=visit)
            failures = exp.failures.format()

            if failures:
                cmd.warn(fileIds)
                cmd.fail(f'text="{exp.failures.format()}"')
            else:
                cmd.finish(fileIds)

        finally:
            exp.exit()
            self.exp.pop(visit, None)

    def doErase(self, cmd):
        """ Move multiple ccdMotors synchronously. """
        cmdKeys = cmd.cmd.keywords

        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else None
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        cams = self.actor.spsConfig.identify(cams=cams)

        nircam = [cam for cam in cams if cam.arm == 'n']

        if len(nircam):
            cams = set(cams) - set(nircam)
            cmd.warn('text="ignoring nir cameras for erase."')

        syncCmd = sync.CcdErase(self.actor, cams=cams)
        syncCmd.process(cmd)

    def abort(self, cmd):
        """Abort current exposure."""
        cmdKeys = cmd.cmd.keywords
        visit = cmdKeys['visit'].values[0]

        try:
            exposure = self.exp[visit]
        except KeyError:
            cmd.fail(f'text="visit:{visit} is not ongoing, valids:{",".join(map(str, self.exp.keys()))} "')
            return

        # nothing will be exposed after an abort, so no illuminator run outlives this one.
        exposure.isLast = True
        exposure.finish(cmd)
        cmd.finish('text="aborting exposure now !"')

    def finish(self, cmd):
        """Finish current exposure."""
        cmdKeys = cmd.cmd.keywords
        visit = cmdKeys['visit'].values[0]

        try:
            exposure = self.exp[visit]
        except KeyError:
            cmd.fail(f'text="visit:{visit} is not ongoing, valids:{",".join(map(str, self.exp.keys()))} "')
            return

        # finishing an exposure by hand ends it, so no illuminator run outlives this one.
        exposure.isLast = True
        exposure.finish(cmd)
        cmd.finish('text="exposure finalizing now..."')

    def status(self, cmd):
        for visit, exp in self.exp.items():
            cmd.inform(f'text="Exposure(visit={visit} exptype={exp.exptype} exptime={exp.exptime}"')

        cmd.finish()
