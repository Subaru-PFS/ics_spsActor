#!/usr/bin/env python

from importlib import reload

import opscore.protocols.keys as keys
import opscore.protocols.types as types
import spsActor.Commands.cmdList as sync
from ics.utils.threading import singleShot
import spsActor.utils.driftSlitExposure.exposure as driftSlitExposure
import spsActor.utils.driftSlitExposure.lampExposure as driftSlitLampExposure

from spsActor.utils import exposure, lampsExposure
from functools import partial
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

        expArgs = '[<visit>] [<cam>] [<cams>] [@doTest] [@doScienceCheck]'
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

            ('erase', f'[<cam>] [<cams>]', self.doErase),

            ('exposure', 'abort <visit>', self.abort),
            ('exposure', 'finish <visit>', self.finish),
            ('exposure', 'status', self.status)
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("sps_expose", (1, 1),
                                        keys.Key("exptime", types.Float(), help="The exposure time"),
                                        keys.Key("cam", types.String(),
                                                 help='single camera to take exposure from'),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        keys.Key("visit", types.Int(),
                                                 help='PFS visit id'),
                                        keys.Key("window", types.Int() * (1, 2),
                                                 help='first row, total number of rows to read, br arms'),
                                        keys.Key("blueWindow", types.Int() * (1, 2),
                                                 help='first row, total number of rows to read on blue arm'),
                                        keys.Key("redWindow", types.Int() * (1, 2),
                                                 help='first row, total number of rows to read on red arm'),
                                        keys.Key('slideSlit', types.Float() * (1, 2),
                                                 help='pixels range(start, stop )')
                                        )

    def doExposure(self, cmd):
        def slitInHome(cams, cmd):
            """Return True if all slits are in home position."""
            notInHome = []

            for specNum in set([cam.specNum for cam in cams]):
                slitPosition = self.actor.models[f'enu_sm{specNum}'].keyVarDict['slitPosition'].getValue()

                if slitPosition != 'home':
                    notInHome.append(f'sm{specNum}={slitPosition}')

            if notInHome:
                cmd.fail(f'text="SlitPositionError({" ".join(notInHome)})"')

            return not len(notInHome)

        cmdKeys = cmd.cmd.keywords

        exptype = None
        blueWindow = redWindow = False

        for valid in ExposeCmd.expTypes:
            exptype = valid if valid in cmdKeys else exptype

        exptime = cmdKeys['exptime'].values[0] if exptype != 'bias' else 0
        visit = cmdKeys['visit'].values[0] if 'visit' in cmdKeys else self.actor.getVisit(cmd=cmd)

        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else None
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        cams = self.actor.spsConfig.identify(cams=cams)

        doLamps = 'doLamps' in cmdKeys
        doShutterTiming = 'doShutterTiming'  in cmdKeys
        doIIS = 'doIIS' in cmdKeys
        doTest = 'doTest' in cmdKeys
        doScienceCheck = 'doScienceCheck' in cmdKeys
        doSlideSlit = 'slideSlit' in cmdKeys
        slideSlitPixelRange = cmdKeys['slideSlit'].values if doSlideSlit else False

        if 'window' in cmdKeys:
            blueWindow = redWindow = cmdKeys['window'].values

        blueWindow = cmdKeys['blueWindow'].values if 'blueWindow' in cmdKeys else blueWindow
        redWindow = cmdKeys['redWindow'].values if 'redWindow' in cmdKeys else redWindow

        nircam = [cam for cam in cams if cam.arm == 'n']

        if len(nircam) and (blueWindow or redWindow):
            cams = set(cams) - set(nircam)
            cmd.warn('text="ignoring nir cameras for windowed exposure."')

        # science check boils down to checking slit position right now, but more to come.
        if doScienceCheck and not slitInHome(cams, cmd=cmd):
            return

        self.process(cmd, visit,
                     exptype=exptype, exptime=exptime, cams=cams, doLamps=doLamps,
                     doShutterTiming=doShutterTiming, doSlideSlit=doSlideSlit, doIIS=doIIS, doTest=doTest,
                     blueWindow=blueWindow, redWindow=redWindow, slideSlitPixelRange=slideSlitPixelRange)

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
        elif doLamps:
            cls = lampsExposure.Exposure
        elif doShutterTiming:
            cls = lampsExposure.ShutterExposure
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

        exposure.finish(cmd)
        cmd.finish('text="exposure finalizing now..."')

    def status(self, cmd):
        for visit, exp in self.exp.items():
            cmd.inform(f'text="Exposure(visit={visit} exptype={exp.exptype} exptime={exp.exptime}"')

        cmd.finish()
