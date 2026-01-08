# !/usr/bin/env python

import argparse
import logging
import time

import actorcore.ICC
from ics.utils.sps.config import SpsConfig
from ics.utils.sps.spectroIds import getSite
from pfs.utils.database import opdb
from pfscore.gen2 import fetchVisitFromGen2
from spsActor.utils.callbacks import MetaStatus


class SpsActor(actorcore.ICC.ICC):
    def __init__(self, name, productName=None, configFile=None, logLevel=logging.INFO):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        self.name = name
        self.site = getSite()

        actorcore.ICC.ICC.__init__(self,
                                   name,
                                   productName=productName,
                                   configFile=configFile)

        self.logger.setLevel(logLevel)
        self.everConnected = False
        self.spsConfig = None
        self.opdb = None
        self.metaStatus = MetaStatus(self)

    def crudeCall(self, cmd, actor, cmdStr, timeLim=60, **kwargs):
        """ crude actor call wrapper. """
        return self.cmdr.call(actor=actor, cmdStr=cmdStr.strip(), timeLim=timeLim, forUserCmd=cmd, **kwargs)

    def safeCall(self, cmd, actor, cmdStr, timeLim=60, **kwargs):
        """ call and throw warnings. """
        cmdVar = self.crudeCall(cmd, actor, cmdStr, timeLim=timeLim, **kwargs)

        if cmdVar.didFail:
            reply = cmdVar.replyList[-1]
            repStr = reply.keywords.canonical(delimiter=';')
            cmdHead = cmdStr.split(" ", 1)[0]
            cmd.warn(repStr.replace('command failed', f'{actor} {cmdHead} failed'))

        return cmdVar

    def requireModels(self, actorList, cmd=None):
        """ Make sure that we are listening for a given actor keywords. """
        cmd = self.bcast if cmd is None else cmd
        actorList = [actorName for actorName in actorList if actorName not in self.models.keys()]

        if actorList:
            cmd.inform(f"text='connecting model for actors {','.join(actorList)}'")
            self.addModels(actorList)
            time.sleep(1)

    def getVisit(self, cmd):
        """ Get visit from gen2 or get your own basically. """
        return fetchVisitFromGen2(self, cmd)

    def reloadConfiguration(self, cmd):
        """ when reloading configuration file, reload spsConfig and status callbacks. """
        self.genSpsKeys(cmd)
        self.metaStatus.attachCallbacks()
        self.opdb = opdb.OpDB()

    def genSpsKeys(self, cmd):
        """ Generate sps config keywords. """
        spsConfig = SpsConfig.fromConfig(self)

        for specModule in spsConfig.values():
            cmd.inform(specModule.genSpecParts)
            cmd.inform(specModule.genLightSource)
            actors = [specModule.enuName] + [cam.actorName for cam in specModule.getCams(filter='operational')]
            if specModule.lightSource.lampsActor:
                actors.append(specModule.lightSource.lampsActor)

            self.requireModels(actors, cmd)

        cmd.inform(f"""specModules={','.join(spsConfig.keys())}""")
        cmd.inform(f"""spsModules={','.join(spsConfig.spsModules.keys())}""")

        # get all cams.
        default = spsConfig.identify(filter='default')
        available = spsConfig.identify(filter='operational')

        defaultCams = ','.join(map(str, default)) if default else 'none'
        availableCams = ','.join(map(str, available)) if available else 'none'

        cmd.inform(f"defaultCams={defaultCams}")
        cmd.inform(f"availableCams={availableCams}")

        self.spsConfig = spsConfig

    def connectionMade(self):
        if self.everConnected is False:
            self.requireModels(['gen2'])
            self.reloadConfiguration(self.bcast)
            self.everConnected = True

    def insert(self, table, cmd=None, **kwargs):
        cmd = self.bcast if cmd is None else cmd

        try:
            self.opdb.insert_kw(table, **kwargs)
        except Exception as e:
            cmd.warn('text=%s' % self.strTraceback(e))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=None, type=str, nargs='?',
                        help='configuration file to use')
    parser.add_argument('--logLevel', default=logging.INFO, type=int, nargs='?',
                        help='logging level')
    parser.add_argument('--name', default='sps', type=str, nargs='?',
                        help='identity')
    args = parser.parse_args()

    theActor = SpsActor(args.name,
                        productName='spsActor',
                        configFile=args.config,
                        logLevel=args.logLevel)
    theActor.run()


if __name__ == '__main__':
    main()
