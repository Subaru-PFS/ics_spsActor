# !/usr/bin/env python

import argparse
import logging
import time

import actorcore.ICC
from pfs.utils.instdata import InstData
from pfs.utils.spectroIds import SpectroIds
from pfs.utils.sps.config import SpsConfig
from pfscore.gen2 import fetchVisitFromGen2
from spsActor.utils.lib import parse


class SpsActor(actorcore.ICC.ICC):
    validCams = [SpectroIds(f'{arm}{specNum}') for arm in SpectroIds.validArms for specNum in SpectroIds.validModules]

    def __init__(self, name, productName=None, configFile=None, logLevel=logging.INFO):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        self.name = name
        actorcore.ICC.ICC.__init__(self,
                                   name,
                                   productName=productName,
                                   configFile=configFile)

        self.logger.setLevel(logLevel)
        self.everConnected = False
        self.instData = InstData(self)
        self.spsConfig = None

    @property
    def cams(self):
        return [c.strip() for c in self.config.get('sps', 'cams').split(',')]

    def crudeCall(self, cmd, actor, cmdStr, timeLim=60, **kwargs):
        cmdStr = parse(cmdStr, **kwargs)
        return self.cmdr.call(actor=actor, cmdStr=cmdStr, timeLim=timeLim, forUserCmd=cmd)

    def safeCall(self, cmd, actor, cmdStr, timeLim=60, **kwargs):
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
        return fetchVisitFromGen2(self, cmd)

    def specFromNum(self, specNum, armNum):
        [cam] = [cam for cam in self.validCams if (cam.specNum == int(specNum) and cam.armNum == int(armNum))]
        return cam

    def getMask(self, frames):
        mask = 0

        for cam in self.validCams:
            bit = cam.camId - 1
            mask |= (1 << bit if cam.camName in frames else 0)

        return mask

    def genSpsKeys(self, cmd):
        spsConfig = SpsConfig.fromConfig(self)

        cmd.inform(f"""specModules={','.join(spsConfig.specModules.keys())}""")
        cmd.inform(f"""spsModules={','.join(spsConfig.spsModules.keys())}""")
        for specModule in spsConfig.specModules.values():
            cmd.inform(specModule.genSpecParts)
            cmd.inform(specModule.genLightSource)
            self.addModels([f'enu_{specModule.specName}'])

        self.spsConfig = spsConfig

    def connectionMade(self):
        if self.everConnected is False:
            self.allControllers = [s.strip() for s in self.config.get(self.name, 'startingControllers').split(',')]

            if any(self.allControllers):
                logging.info("Attaching Controllers")
                self.attachAllControllers()
                logging.info("All Controllers started")

            self.everConnected = True


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
