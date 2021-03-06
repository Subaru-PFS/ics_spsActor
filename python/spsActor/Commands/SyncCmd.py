#!/usr/bin/env python


import opscore.protocols.keys as keys
import opscore.protocols.types as types
from spsActor.utils.sync import Sync


class SyncCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a le   le argument, the parsed and typed command.
        #
        self.name = "sync"
        self.vocab = [
            ('slit', '<focus> [@(microns)] [@(abs)] [<cams>]', self.slitFocus),
            ('slit', 'dither [<x>] [<y>] [@(pixels|microns)] [@(abs)] [<cams>]', self.slitDither),
            ('ccdMotors', 'move [<a>] [<b>] [<c>] [<piston>] [@(microns)] [@(abs)] [<cams>]', self.ccdMotors),
            ('iis', '[<on>] [<warmingTime>] [<cams>]', self.iisOn),
            ('iis', '<off> [<cams>]', self.iisOff),
            ('checkFocus', '[<cams>]', self.checkFocus),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("sps_sync", (1, 1),
                                        keys.Key('focus', types.Float(), help='focus value'),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        keys.Key("a", types.Float(),
                                                 help='the number of ticks/microns to move actuator A'),
                                        keys.Key("b", types.Float(),
                                                 help='the number of ticks/microns to move actuator B'),
                                        keys.Key("c", types.Float(),
                                                 help='the number of ticks/microns to move actuator C'),
                                        keys.Key("piston", types.Float(),
                                                 help='the number of ticks/microns to move actuators A,B, and C'),
                                        keys.Key("x", types.Float(),
                                                 help='dither in pixels wrt ccd x direction'),
                                        keys.Key("y", types.Float(),
                                                 help='dither in pixels wrt ccd y direction'),
                                        keys.Key('on', types.String() * (1, None),
                                                 help='which iis lamp to switch on.'),
                                        keys.Key('off', types.String() * (1, None),
                                                 help='which iis lamp to switch off.'),
                                        keys.Key('warmingTime', types.Float(), help='customizable warming time'),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers[self.name]
        except KeyError:
            raise RuntimeError('%s controller is not connected.' % self.name)

    def slitFocus(self, cmd):
        """ Focus multiple slits synchronously. """
        cams = self.actor.cams
        cmdKeys = cmd.cmd.keywords
        focus = cmdKeys['focus'].values[0]
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        microns = 'microns' in cmdKeys
        abs = 'abs' in cmdKeys
        specNums = list(set([int(cam[-1]) for cam in cams]))

        syncCmd = Sync.slit(self.actor, specNums=specNums, cmdHead='', focus=focus, microns=microns, abs=abs)

        cams = list(syncCmd.process(cmd) & set(cams))
        if not cams:
            cmd.fail('text="failed to focus any slit"')
            return

        cmd.finish(f'exposable={",".join(cams)}')

    def slitDither(self, cmd):
        """ Dither multiple slits synchronously. """
        cams = self.actor.cams
        cmdKeys = cmd.cmd.keywords
        ditherX = cmdKeys['x'].values[0] if 'x' in cmdKeys else None
        ditherY = cmdKeys['y'].values[0] if 'y' in cmdKeys else None
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        microns = 'microns' in cmdKeys
        pixels = 'pixels' in cmdKeys
        abs = 'abs' in cmdKeys

        specNums = list(set([int(cam[-1]) for cam in cams]))

        syncCmd = Sync.slit(self.actor, specNums=specNums, cmdHead='dither',
                            x=ditherX, y=ditherY, microns=microns, pixels=pixels, abs=abs)

        cams = list(syncCmd.process(cmd) & set(cams))
        if not cams:
            cmd.fail('text="failed to dither any slit"')
            return

        cmd.finish(f'exposable={",".join(cams)}')

    def ccdMotors(self, cmd):
        """ Move multiple ccdMotors synchronously. """
        cams = self.actor.cams
        cmdKeys = cmd.cmd.keywords
        a = cmdKeys['a'].values[0] if 'a' in cmdKeys else None
        b = cmdKeys['b'].values[0] if 'b' in cmdKeys else None
        c = cmdKeys['c'].values[0] if 'c' in cmdKeys else None
        piston = cmdKeys['piston'].values[0] if 'piston' in cmdKeys else None
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        microns = 'microns' in cmdKeys
        abs = 'abs' in cmdKeys

        syncCmd = Sync.ccdMotors(self.actor, cams=cams, cmdHead='move',
                                 a=a, b=b, c=c, piston=piston, microns=microns, abs=abs)

        cams = list(syncCmd.process(cmd) & set(cams))
        if not cams:
            cmd.fail('text="failed to command any ccd focus motors"')
            return

        cmd.finish(f'exposable={",".join(cams)}')

    def iisOn(self, cmd):
        """ Turn multiple iis on synchronously. """
        cams = self.actor.cams
        cmdKeys = cmd.cmd.keywords
        on = ','.join(cmdKeys['on'].values) if 'on' in cmdKeys else None
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        warmingTime = cmdKeys['warmingTime'].values[0] if 'warmingTime' in cmdKeys else None
        timeLim = warmingTime if warmingTime is not None else 60
        specNums = list(set([int(cam[-1]) for cam in cams]))

        syncCmd = Sync.iis(self.actor, specNums=specNums, cmdHead='',
                           on=on, warmingTime=warmingTime, timeLim=timeLim + 30)

        cams = list(syncCmd.process(cmd) & set(cams))
        if not cams:
            cmd.fail('text="failed to command iis"')
            return

        cmd.finish(f'exposable={",".join(cams)}')

    def iisOff(self, cmd):
        """ Turn multiple iis off synchronously. """
        cams = self.actor.cams
        cmdKeys = cmd.cmd.keywords
        off = ','.join(cmdKeys['off'].values)
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams
        specNums = list(set([int(cam[-1]) for cam in cams]))

        syncCmd = Sync.iis(self.actor, specNums=specNums, cmdHead='', off=off)

        cams = list(syncCmd.process(cmd) & set(cams))
        if not cams:
            cmd.fail('text="failed to command iis"')
            return

        cmd.finish()

    def checkFocus(self, cmd):
        """ Focus multiple slits synchronously. """
        cams = self.actor.cams
        cmdKeys = cmd.cmd.keywords
        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else cams

        specNums = list(set([int(cam[-1]) for cam in cams]))
        models = [f'xcu_{cam}' for cam in cams] + [f'enu_sm{specNum}' for specNum in specNums]

        self.actor.requireModels(models, cmd=cmd)

        for specNum in specNums:
            slitPosition = self.actor.models[f'enu_sm{specNum}'].keyVarDict['slitPosition'].valueList[0]
            if slitPosition != 'home':
                raise ValueError(f'sm{specNum} slit is not focused')

        cmd.finish()
