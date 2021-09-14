#!/usr/bin/env python

from importlib import reload

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr
from spsActor.utils import exposure, lampsExposure
from spsActor.utils.lib import singleShot, wait

reload(exposure)


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
        exptypes = "|".join(ExposeCmd.expTypes[1:])
        self.exp = dict()

        self.vocab = [
            ('expose', f'@({exptypes}) <exptime> [<visit>] [<cam>] [<cams>] [@doLamps] [@doTest]', self.doExposure),
            ('expose', 'bias [<visit>] [<cam>] [<cams>] [doTest]', self.doExposure),
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
                                        )

    def doExposure(self, cmd):
        cmdKeys = cmd.cmd.keywords

        exptype = None

        for valid in ExposeCmd.expTypes:
            exptype = valid if valid in cmdKeys else exptype

        exptime = cmdKeys['exptime'].values[0] if exptype is not 'bias' else 0
        visit = cmdKeys['visit'].values[0] if 'visit' in cmdKeys else self.actor.getVisit(cmd=cmd)

        cams = self.actor.cams
        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else cams
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        models = [f'ccd_{cam}' for cam in cams] + list(set([f'enu_sm{i}' for i in [int(cam[-1]) for cam in cams]]))

        doLamps = 'doLamps' in cmdKeys
        doTest = 'doTest' in cmdKeys

        self.actor.requireModels(models, cmd=cmd)
        self.process(cmd, visit, exptype=exptype, exptime=exptime, cams=cams, doLamps=doLamps, doTest=doTest)

    @singleShot
    def process(self, cmd, visit, exptype, doLamps, **kwargs):
        """Process exposure in another thread """

        def genFileIds(visit, frames):
            return f"""fileIds={visit},{qstr(';'.join(frames))},0x{self.actor.getMask(frames):04x}"""

        if visit in self.exp.keys():
            cmd.fail(f'text="exposure(visit={visit}) already ongoing"')
            return

        if exptype in ['bias', 'dark']:
            cls = exposure.DarkExposure

        elif doLamps:
            cls = lampsExposure.Exposure

        else:
            cls = exposure.Exposure

        exp = cls(self.actor, exptype=exptype, **kwargs)
        self.exp[visit] = exp

        try:
            exp.start(cmd, visit)

            while not exp.isFinished:
                wait()

            if any(exp.clearedExp):
                if exp.storable:
                    frames = exp.store(cmd, visit)
                    cmd.warn(genFileIds(visit, frames))
                cmd.fail(f'text="{exp.failures.format()}"')
            else:
                frames = exp.store(cmd, visit)
                cmd.finish(genFileIds(visit, frames))

        finally:
            exp.exit()
            self.exp.pop(visit, None)

    def abort(self, cmd):
        """Abort current exposure."""
        cmdKeys = cmd.cmd.keywords
        visit = cmdKeys['visit'].values[0]

        try:
            exposure = self.exp[visit]
        except KeyError:
            cmd.fail(f'text="visit:{visit} is not ongoing, valids:{",".join(map(str, self.exp.keys()))} "')
            return

        exposure.abort(cmd)
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
