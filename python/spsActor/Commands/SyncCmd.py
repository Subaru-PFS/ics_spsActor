#!/usr/bin/env python

import ics.utils.sps.lamps.utils.lampState as lampState
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
        spsArgs = '[<cam>] [<cams>] [<specNum>] [<specNums>] [<arm>] [<arms>]'
        self.vocab = [
            ('slit', f'<focus> [@(microns)] [@(abs)] {spsArgs}', self.slitFocus),
            ('slit', f'dither [<x>] [<y>] [@(pixels|microns)] [@(abs)] {spsArgs}', self.slitDither),
            ('slit', f'home {spsArgs}', self.slitHome),
            ('slit', f'start {spsArgs}', self.slitStart),
            ('slit', f'stop {spsArgs}', self.slitStop),

            ('rda', f'@moveTo @(low|med) {spsArgs}', self.rdaMove),

            ('bia', f'@on [strobe] [<power>] [<duty>] [<period>] {spsArgs}', self.biaSwitchOn),
            ('bia', f'@off {spsArgs}', self.biaSwitchOff),
            ('bia', f'@strobe @off {spsArgs}', self.biaSwitchOff),

            ('iis', f'<on> [<warmingTime>] {spsArgs}', self.iisOn),
            ('iis', f'<off> {spsArgs}', self.iisOff),
            ('iis', f'prepare [<halogen>] [<argon>] [<hgar>] [<neon>] [<krypton>] {spsArgs}', self.iisPrepare),
            ('ccdMotors', f'move [<a>] [<b>] [<c>] [<piston>] [@(microns)] [@(abs)] {spsArgs}', self.ccdMotors),
            ('fpa', f'toFocus {spsArgs}', self.fpaToFocus),
            ('fpa', f'moveFocus [<microns>] [@(abs)] {spsArgs}', self.fpaMoveFocus),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("sps_sync", (1, 1),
                                        keys.Key('focus', types.Float(), help='focus value'),
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
                                        keys.Key("a", types.Float(),
                                                 help='the number of ticks/microns to move actuator A'),
                                        keys.Key("b", types.Float(),
                                                 help='the number of ticks/microns to move actuator B'),
                                        keys.Key("c", types.Float(),
                                                 help='the number of ticks/microns to move actuator C'),
                                        keys.Key("piston", types.Float(),
                                                 help='the number of ticks/microns to move actuators A,B, and C'),
                                        keys.Key("microns", types.Float(),
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

                                        keys.Key('halogen', types.Float(), help='quartz halogen lamp on time'),
                                        keys.Key('argon', types.Float(), help='Ar lamp on time'),
                                        keys.Key('hgar', types.Float(), help='HgAr lamp on time'),
                                        keys.Key('neon', types.Float(), help='Ne lamp on time'),
                                        keys.Key('krypton', types.Float(), help='Kr lamp on time'),
                                        )

    @property
    def controller(self):
        try:
            return self.actor.controllers[self.name]
        except KeyError:
            raise RuntimeError('%s controller is not connected.' % self.name)

    @singleShot
    def slitFocus(self, cmd):
        """Focus multiple slits synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

        focus = cmdKeys['focus'].values[0]
        microns = 'microns' in cmdKeys
        abs = 'abs' in cmdKeys

        syncCmd = sync.SlitMove(self.actor, specNums=specNums, cmdHead='', focus=focus, microns=microns, abs=abs)
        syncCmd.process(cmd)

    @singleShot
    def slitDither(self, cmd):
        """Dither multiple slits synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

        ditherX = cmdKeys['x'].values[0] if 'x' in cmdKeys else None
        ditherY = cmdKeys['y'].values[0] if 'y' in cmdKeys else None
        microns = 'microns' in cmdKeys
        pixels = 'pixels' in cmdKeys
        abs = 'abs' in cmdKeys

        syncCmd = sync.SlitMove(self.actor, specNums=specNums, cmdHead='dither',
                                x=ditherX, y=ditherY, microns=microns, pixels=pixels, abs=abs)
        syncCmd.process(cmd)

    @singleShot
    def slitHome(self, cmd):
        """Move all slits to home."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

        syncCmd = sync.SlitMove(self.actor, specNums=specNums, cmdHead='home')
        syncCmd.process(cmd)

    @singleShot
    def slitStart(self, cmd):
        """Start all slits."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

        syncCmd = sync.SlitStart(self.actor, specNums=specNums)
        syncCmd.process(cmd)

    @singleShot
    def slitStop(self, cmd):
        """Stop all slits"""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

        syncCmd = sync.SlitStop(self.actor, specNums=specNums)
        syncCmd.process(cmd)

    @singleShot
    def rdaMove(self, cmd):
        """Move multiple rda synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

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
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)

        power = cmdKeys['power'].values[0] if 'power' in cmdKeys else None
        period = cmdKeys['period'].values[0] if 'period' in cmdKeys else None
        duty = cmdKeys['duty'].values[0] if 'duty' in cmdKeys else None
        strobe = 'strobe' in cmdKeys

        syncCmd = sync.BiaSwitch(self.actor, state='on', specNums=specNums,
                                 strobe=strobe, power=power, period=period, duty=duty)
        syncCmd.process(cmd)

    @singleShot
    def biaSwitchOff(self, cmd):
        """Switch multiple bia synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)
        state = 'strobe off' if 'strobe' in cmdKeys else 'off'

        syncCmd = sync.BiaSwitch(self.actor, state=state, specNums=specNums)
        syncCmd.process(cmd)

    @singleShot
    def ccdMotors(self, cmd):
        """Move multiple ccdMotors synchronously."""
        cmdKeys, cams = self.actor.spsConfig.keysToCam(cmd)

        a = cmdKeys['a'].values[0] if 'a' in cmdKeys else None
        b = cmdKeys['b'].values[0] if 'b' in cmdKeys else None
        c = cmdKeys['c'].values[0] if 'c' in cmdKeys else None
        piston = cmdKeys['piston'].values[0] if 'piston' in cmdKeys else None
        microns = 'microns' in cmdKeys
        abs = 'abs' in cmdKeys

        syncCmd = sync.FpaMove(self.actor, cams=cams, cmdHead='move',
                               a=a, b=b, c=c, piston=piston, microns=microns, abs=abs)
        syncCmd.process(cmd)

    @singleShot
    def fpaToFocus(self, cmd):
        """Move multiple ccdMotors synchronously."""
        cmdKeys, cams = self.actor.spsConfig.keysToCam(cmd)

        syncCmd = sync.FpaMove(self.actor, cams=cams, cmdHead='toFocus')
        syncCmd.process(cmd)

    @singleShot
    def fpaMoveFocus(self, cmd):
        """Move multiple ccdMotors synchronously."""
        cmdKeys, cams = self.actor.spsConfig.keysToCam(cmd)

        microns = cmdKeys['microns'].values[0] if 'microns' in cmdKeys else None
        abs = 'abs' in cmdKeys

        syncCmd = sync.FpaMove(self.actor, cams=cams, microns=microns, abs=abs, cmdHead='moveFocus')
        syncCmd.process(cmd)

    @singleShot
    def iisOn(self, cmd):
        """Turn multiple iis on synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)
        on = cmdKeys['on'].values
        warmingTime = cmdKeys['warmingTime'].values[0] if 'warmingTime' in cmdKeys else False

        syncCmd = sync.IisOn(self.actor, specNums=specNums, on=on, warmingTime=warmingTime)
        syncCmd.process(cmd)

    @singleShot
    def iisOff(self, cmd):
        """Turn multiple iis off synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)
        off = cmdKeys['off'].values

        syncCmd = sync.IisOff(self.actor, specNums=specNums, off=off)
        syncCmd.process(cmd)

    @singleShot
    def iisPrepare(self, cmd):
        """Turn multiple iis off synchronously."""
        cmdKeys, specNums = self.actor.spsConfig.keysToSpecNum(cmd)
        lampKeys = {name: int(round(cmdKeys[name].values[0])) for name in lampState.allLamps if name in cmdKeys}

        syncCmd = sync.IisPrepare(self.actor, specNums=specNums, **lampKeys)
        syncCmd.process(cmd)
