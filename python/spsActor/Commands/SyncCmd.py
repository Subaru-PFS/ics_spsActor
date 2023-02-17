#!/usr/bin/env python


import opscore.protocols.keys as keys
import opscore.protocols.types as types
import spsActor.Commands.cmdList as sync
from ics.utils.threading import singleShot


class SyncCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor
        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.name = "sync"
        self.vocab = [
            ('slit', '<focus> [@(microns)] [@(abs)] [<specNums>] [<cams>]', self.slitFocus),
            ('slit', 'dither [<x>] [<y>] [@(pixels|microns)] [@(abs)] [<specNums>] [<cams>]', self.slitDither),
            ('slit', 'home [<specNums>] [<cams>]', self.slitHome),
            ('slit', 'start [<specNums>] [<cams>]', self.slitStart),
            ('slit', 'stop [<specNums>] [<cams>]', self.slitStop),

            ('rda', '@moveTo @(low|med) [<specNums>] [<cams>]', self.rdaMove),

            ('bia', '@on [strobe] [<power>] [<duty>] [<period>] [<specNums>] [<cams>]', self.biaSwitchOn),
            ('bia', '@off [<specNums>] [<cams>]', self.biaSwitchOff),
            ('bia', '@strobe @off [<specNums>] [<cams>]', self.biaSwitchOff),

            ('iis', '[<on>] [<warmingTime>] [<cams>]', self.iisOn),
            ('iis', '<off> [<cams>]', self.iisOff),

            ('ccdMotors', 'move [<a>] [<b>] [<c>] [<piston>] [@(microns)] [@(abs)] [<cams>]', self.ccdMotors),
            ('ccdMotors', 'toFocus [<cams>]', self.ccdMotorsToFocus),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("sps_sync", (1, 1),
                                        keys.Key('focus', types.Float(), help='focus value'),
                                        keys.Key("cams", types.String() * (1,),
                                                 help='list of camera to take exposure from'),
                                        keys.Key('specNums', types.Int() * (1,),
                                                 help='spectrograph module(s)'),
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
                                        keys.Key('period', types.Int(), help='bia period'),
                                        keys.Key("power", types.Float(), help='power level to set (0..100)'),
                                        keys.Key("duty", types.Int(), help='strobe duty cycle (0..100)'),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers[self.name]
        except KeyError:
            raise RuntimeError('%s controller is not connected.' % self.name)

    def findSpecNums(self, cmdKeys):
        """Get specNum from cmdKeys if specified, get values from spsConfig otherwise.

       Parameters
       ----------
       cmdKeys : `str`
           camera used to measure fiducials position.
       """
        if 'cams' in cmdKeys:
            specNums = list(set([int(cam[-1]) for cam in cmdKeys['cams'].values]))
        elif 'specNums' in cmdKeys:
            specNums = list(map(int, cmdKeys['specNums'].values))
        else:
            specNums = [specModule.specNum for specModule in self.actor.spsConfig.selectModules()]

        return specNums

    @singleShot
    def slitFocus(self, cmd):
        """Focus multiple slits synchronously."""
        cmdKeys = cmd.cmd.keywords

        focus = cmdKeys['focus'].values[0]
        microns = 'microns' in cmdKeys
        abs = 'abs' in cmdKeys
        specNums = self.findSpecNums(cmdKeys)

        syncCmd = sync.SlitMove(self.actor, specNums=specNums, cmdHead='', focus=focus, microns=microns, abs=abs)
        syncCmd.process(cmd)

    @singleShot
    def slitDither(self, cmd):
        """Dither multiple slits synchronously."""
        cmdKeys = cmd.cmd.keywords

        ditherX = cmdKeys['x'].values[0] if 'x' in cmdKeys else None
        ditherY = cmdKeys['y'].values[0] if 'y' in cmdKeys else None
        microns = 'microns' in cmdKeys
        pixels = 'pixels' in cmdKeys
        abs = 'abs' in cmdKeys
        specNums = self.findSpecNums(cmdKeys)

        syncCmd = sync.SlitMove(self.actor, specNums=specNums, cmdHead='dither',
                                x=ditherX, y=ditherY, microns=microns, pixels=pixels, abs=abs)
        syncCmd.process(cmd)

    @singleShot
    def slitHome(self, cmd):
        """Move all slits to home."""
        cmdKeys = cmd.cmd.keywords
        specNums = self.findSpecNums(cmdKeys)

        syncCmd = sync.SlitMove(self.actor, specNums=specNums, cmdHead='home')
        syncCmd.process(cmd)

    @singleShot
    def slitStart(self, cmd):
        """Start all slits."""
        cmdKeys = cmd.cmd.keywords
        specNums = self.findSpecNums(cmdKeys)

        syncCmd = sync.SlitStart(self.actor, specNums=specNums)
        syncCmd.process(cmd)

    @singleShot
    def slitStop(self, cmd):
        """Stop all slits"""
        cmdKeys = cmd.cmd.keywords
        specNums = self.findSpecNums(cmdKeys)

        syncCmd = sync.SlitStop(self.actor, specNums=specNums)
        syncCmd.process(cmd)

    @singleShot
    def rdaMove(self, cmd):
        """Move multiple rda synchronously."""
        cmdKeys = cmd.cmd.keywords
        specNums = self.findSpecNums(cmdKeys)

        if 'low' in cmdKeys:
            targetPosition = 'low'
        elif 'med' in cmdKeys:
            targetPosition = 'med'
        else:
            raise ValueError('incorrect target position')

        syncCmd = sync.RdaMove(self.actor, specNums=specNums, targetPosition=targetPosition)
        syncCmd.process(cmd)

    @singleShot
    def biaSwitchOn(self, cmd):
        """Switch multiple bia synchronously."""
        cmdKeys = cmd.cmd.keywords

        power = cmdKeys['power'].values[0] if 'power' in cmdKeys else None
        period = cmdKeys['period'].values[0] if 'period' in cmdKeys else None
        duty = cmdKeys['duty'].values[0] if 'duty' in cmdKeys else None
        strobe = 'strobe' in cmdKeys
        specNums = self.findSpecNums(cmdKeys)

        syncCmd = sync.BiaSwitch(self.actor, state='on', specNums=specNums,
                                 strobe=strobe, power=power, period=period, duty=duty)
        syncCmd.process(cmd)

    @singleShot
    def biaSwitchOff(self, cmd):
        """Switch multiple bia synchronously."""
        cmdKeys = cmd.cmd.keywords

        specNums = self.findSpecNums(cmdKeys)
        state = 'strobe off' if 'strobe' in cmdKeys else 'off'

        syncCmd = sync.BiaSwitch(self.actor, state=state, specNums=specNums)
        syncCmd.process(cmd)

    @singleShot
    def ccdMotors(self, cmd):
        """Move multiple ccdMotors synchronously."""
        cmdKeys = cmd.cmd.keywords
        a = cmdKeys['a'].values[0] if 'a' in cmdKeys else None
        b = cmdKeys['b'].values[0] if 'b' in cmdKeys else None
        c = cmdKeys['c'].values[0] if 'c' in cmdKeys else None
        piston = cmdKeys['piston'].values[0] if 'piston' in cmdKeys else None
        microns = 'microns' in cmdKeys
        abs = 'abs' in cmdKeys

        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else None
        cams = self.actor.spsConfig.identify(cams=cams)

        syncCmd = sync.CcdMotorsMove(self.actor, cams=cams, cmdHead='move',
                                     a=a, b=b, c=c, piston=piston, microns=microns, abs=abs)

        syncCmd.process(cmd)

    @singleShot
    def ccdMotorsToFocus(self, cmd):
        """Move multiple ccdMotors synchronously."""
        cmdKeys = cmd.cmd.keywords

        cams = cmdKeys['cams'].values if 'cams' in cmdKeys else None
        cams = self.actor.spsConfig.identify(cams=cams)

        syncCmd = sync.CcdMotorsMove(self.actor, cams=cams, cmdHead='toFocus')
        syncCmd.process(cmd)

    @singleShot
    def iisOn(self, cmd):
        """Turn multiple iis on synchronously."""
        cmdKeys = cmd.cmd.keywords
        specNums = self.findSpecNums(cmdKeys)

        cmd.finish()

    @singleShot
    def iisOff(self, cmd):
        """Turn multiple iis off synchronously."""
        cmdKeys = cmd.cmd.keywords
        specNums = self.findSpecNums(cmdKeys)

        cmd.finish()
