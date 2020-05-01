#!/usr/bin/env python


import opscore.protocols.keys as keys
import opscore.protocols.types as types
from spsActor.Controllers.exposure import Calib, Exposure
from spsActor.utils import singleShot, wait


class ExposeCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a le   le argument, the parsed and typed command.
        #
        self.exp = None
        self.vocab = [
            ('expose', '[@(object|arc|flat|dark)] <exptime> [<visit>] [<cam>] [<cams>]', self.doExposure),
            ('expose', 'bias [<visit>] [<cam>] [<cams>]', self.doExposure),
            ('exposure', 'abort', self.abort),
            ('exposure', 'finish', self.finish),
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

        self.actor.requireModels(models, cmd=cmd)
        self.process(cmd, visit, exptype=exptype, exptime=exptime, cams=cams)

    @singleShot
    def process(self, cmd, visit, exptype, **kwargs):
        """Process exposure in another thread """
        if self.exp is not None:
            cmd.fail('text="exposure already ongoing"')
            return

        cls = Calib if exptype in ['bias', 'dark'] else Exposure
        self.exp = cls(self.actor, exptype=exptype, **kwargs)

        try:
            self.exp.start(cmd, visit)

            while not self.exp.isFinished:
                wait()

            if self.exp.aborted:
                raise RuntimeError('exposure aborted')

            if self.exp.cleared:
                raise RuntimeError('exposure failed')

            frames = self.exp.store(cmd, visit)
            cmd.inform(f'frames={",".join(frames)}')
            cmd.finish(f'visit={visit}')

        finally:
            self.exp.exit()
            self.exp = None

    def abort(self, cmd):
        """Abort current exposure."""
        if self.exp is None:
            cmd.fail('text="no exposure to abort"')
            return

        self.exp.abort(cmd)
        cmd.finish()

    def finish(self, cmd):
        """Finish current exposure."""
        if self.exp is None:
            cmd.fail('text="no exposure to finish"')
            return

        self.exp.finish(cmd)
        cmd.finish('text="exposure finished"')
