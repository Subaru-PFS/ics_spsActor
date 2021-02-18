#!/usr/bin/env python

from importlib import reload

import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr
from spsActor.utils import exposure
from spsActor.utils import singleShot, wait

reload(exposure)


class ExposeCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a le   le argument, the parsed and typed command.
        #
        self.exp = dict()
        self.vocab = [
            ('expose', '@(object|arc|flat|dark) <exptime> [<visit>] [<cam>] [<cams>] [@doLamps]', self.doExposure),
            ('expose', 'bias [<visit>] [<cam>] [<cams>]', self.doExposure),
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

        exptype = 'object'
        exptype = 'arc' if 'arc' in cmdKeys else exptype
        exptype = 'flat' if 'flat' in cmdKeys else exptype
        exptype = 'bias' if 'bias' in cmdKeys else exptype
        exptype = 'dark' if 'dark' in cmdKeys else exptype

        exptime = cmdKeys['exptime'].values[0] if exptype is not 'bias' else 0
        visit = cmdKeys['visit'].values[0] if 'visit' in cmdKeys else self.actor.getVisit(cmd=cmd)

        cams = self.actor.cams
        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else cams
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        models = [f'ccd_{cam}' for cam in cams] + list(set([f'enu_sm{i}' for i in [int(cam[-1]) for cam in cams]]))

        doLamps = 'doLamps' in cmdKeys

        self.actor.requireModels(models, cmd=cmd)
        self.process(cmd, visit, exptype=exptype, exptime=exptime, cams=cams, doLamps=doLamps)

    @singleShot
    def process(self, cmd, visit, exptype, **kwargs):
        """Process exposure in another thread """
        if visit in self.exp.keys():
            cmd.fail(f'text="exposure(visit={visit}) already ongoing"')
            return

        cls = exposure.Calib if exptype in ['bias', 'dark'] else exposure.Exposure
        exp = cls(self.actor, exptype=exptype, **kwargs)
        self.exp[visit] = exp

        try:
            exp.start(cmd, visit)

            while not exp.isFinished:
                wait()

            if exp.cleared:
                if exp.aborted:
                    raise RuntimeError('abort exposure requested...')
                else:
                    raise RuntimeError('exposure failed...')

            frames = exp.store(cmd, visit)
            cmd.finish(f"""fileIds={visit},{qstr(';'.join(frames))},0x{self.actor.getMask(frames):04x}""")

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