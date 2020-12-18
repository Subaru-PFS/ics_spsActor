#!/usr/bin/env python


import opscore.protocols.keys as keys
import opscore.protocols.types as types
from pfs.utils.spectroIds import SpectroIds, SpecModule, SpsConfig


class TopCmd(object):
    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        lightSources = '|'.join(SpecModule.lightSources)
        self.vocab = [
            ('ping', '', self.ping),
            ('status', '', self.status),
            ('declareLightSource', f'[<sm1>] [<sm2>] [<sm3>] [<sm4>] [{lightSources}]', self.declareLightSource),

        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("sps_sps", (1, 1),
                                        keys.Key('sm1', types.String(), help='sm1 light source'),
                                        keys.Key('sm2', types.String(), help='sm2 light source'),
                                        keys.Key('sm3', types.String(), help='sm3 light source'),
                                        keys.Key('sm4', types.String(), help='sm4 light source'),
                                        )

    def ping(self, cmd):
        """Query the actor for liveness/happiness."""

        cmd.finish("text='Present and (probably) well'")

    def status(self, cmd):
        """Report status and version; obtain and send current data"""

        cmd.inform('text="Present!"')
        self.actor.sendVersionKey(cmd)
        self.actor.genSpsKeys(cmd)
        cmd.finish()

    def declareLightSource(self, cmd):
        """Report status and version; obtain and send current data"""
        cmdKeys = cmd.cmd.keywords
        lightSource = False
        spsConfig = SpsConfig.fromConfig(self.actor)

        for specNum in SpectroIds.validModules:
            try:
                lightSource = cmdKeys[f'sm{specNum}'].values[0].strip().lower()
            except KeyError:
                continue
            [specModule] = spsConfig.selectModules([specNum])
            specModule.declareLightSource(lightSource, spsData=self.actor.instData)

        if not lightSource:
            [lightSource] = [source for source in SpecModule.lightSources if source in cmdKeys]
            if len(spsConfig) > 1 and lightSource != 'pfi':
                raise RuntimeError(f'{lightSource} can only be plugged to a single SM')

            for specModule in spsConfig.values():
                specModule.declareLightSource(lightSource, spsData=self.actor.instData)

        self.actor.genSpsKeys(cmd)
        cmd.finish()
