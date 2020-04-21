# !/usr/bin/env python

import argparse
import logging
import time

import actorcore.ICC
from pfscore.gen2 import fetchVisitFromGen2


class SpsActor(actorcore.ICC.ICC):
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

    @property
    def cams(self):
        return [c.strip() for c in self.config.get('sps', 'cams').split(',')]

    def safeCall(self, **kwargs):
        cmd = kwargs["forUserCmd"]
        kwargs["timeLim"] = 300 if "timeLim" not in kwargs.keys() else kwargs["timeLim"]

        cmdVar = self.cmdr.call(**kwargs)

        if cmdVar.didFail:
            reply = cmdVar.replyList[-1]
            repStr = reply.keywords.canonical(delimiter=';')
            cmd.warn(repStr.replace('command failed', f'{kwargs["actor"]} {kwargs["cmdStr"].split(" ", 1)[0]} failed'))

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

    def connectionMade(self):
        if self.everConnected is False:
            logging.info("Attaching Controllers")
            self.allControllers = [s.strip() for s in self.config.get(self.name, 'startingControllers').split(',')]
            self.attachAllControllers()
            self.everConnected = True
            logging.info("All Controllers started")


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
