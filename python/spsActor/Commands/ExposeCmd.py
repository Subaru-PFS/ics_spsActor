#!/usr/bin/env python


import opscore.protocols.keys as keys
import opscore.protocols.types as types
from spsActor.utils import singleShot


class ExposeCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a le   le argument, the parsed and typed command.
        #
        self.name = "expose"
        self.vocab = [
            ('expose', '[@(object|arc|flat|dark)] <exptime> [<visit>] [<cam>] [<cams>]', self.doExposure),
            ('expose', 'bias [<visit>] [<cam>] [<cams>]', self.doExposure),
            ('exposure', 'abort', self.doStop),
            ('exposure', 'finish', self.doFinish)
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("spsait_expose", (1, 1),
                                        keys.Key("exptime", types.Float(), help="The exposure time"),
                                        keys.Key("cam", types.String(),
                                                 help='single camera to take exposure from'),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        keys.Key("visit", types.Int(),
                                                 help='PFS visit id'),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers[self.name]
        except KeyError:
            raise RuntimeError('%s controller is not connected.' % self.name)

    @singleShot
    def doExposure(self, cmd):
        self.controller.resetExposure()
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

        self.controller.expose(cmd=cmd,
                               exptype=exptype,
                               exptime=float(exptime),
                               visit=int(visit),
                               cams=cams)

        cmd.finish('visit=%d' % visit)

    def doStop(self, cmd):
        self.controller.stopExposure(cmd)
        cmd.finish('text="exposure stopped"')

    def doFinish(self, cmd):
        self.controller.finishExposure(cmd)
        cmd.finish('text="exposure finished"')
