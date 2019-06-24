#!/usr/bin/env python


import opscore.protocols.keys as keys
import opscore.protocols.types as types
from spsaitActor.utils import threaded


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
            ('expose', '[@(object|arc|flat)] <exptime> [<cam>] [<cams>]', self.doExposure),
            ('expose', 'bias [<cam>] [<cams>]', self.doBias),
            ('expose', 'dark <exptime> [<cam>] [<cams>]', self.doDark),

        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("spsait_expose", (1, 1),
                                        keys.Key("exptime", types.Float(), help="The exposure time"),
                                        keys.Key("cam", types.String(),
                                                 help='single camera to take exposure from'),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers[self.name]
        except KeyError:
            raise RuntimeError('%s controller is not connected.' % self.name)

    @property
    def doStop(self):
        return self.actor.doStop

    @threaded
    def doExposure(self, cmd):
        cmdKeys = cmd.cmd.keywords
        exptype = 'object'
        exptype = 'arc' if 'arc' in cmdKeys else exptype
        exptype = 'flat' if 'flat' in cmdKeys else exptype

        exptime = cmdKeys['exptime'].values[0]
        cams = False
        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else cams
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams

        visit = self.controller.expose(cmd=cmd,
                                       exptype=exptype,
                                       exptime=exptime,
                                       cams=cams)

        cmd.finish('visit=%i' % visit)

    @threaded
    def doBias(self, cmd):
        cmdKeys = cmd.cmd.keywords
        cams = False
        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else cams
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams

        visit = self.controller.calibExposure(cmd=cmd,
                                              cams=cams,
                                              exptype='bias',
                                              exptime=0)

        cmd.finish('visit=%i' % visit)

    @threaded
    def doDark(self, cmd):
        cmdKeys = cmd.cmd.keywords
        exptime = cmdKeys['exptime'].values[0]
        cams = False
        cams = [cmdKeys['cam'].values[0]] if 'cam' in cmdKeys else cams
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams

        visit = self.controller.calibExposure(cmd=cmd,
                                              cams=cams,
                                              exptype='dark',
                                              exptime=exptime)

        cmd.finish('visit=%i' % visit)
