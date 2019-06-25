# !/usr/bin/env python

import argparse
import logging

import actorcore.ICC


class SpsActor(actorcore.ICC.ICC):
    def __init__(self, name, productName=None, configFile=None, logLevel=logging.INFO):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        self.name = name

        specIds = [i + 1 for i in range(4)]
        allcams = ['b%i' % i for i in specIds] + ['r%i' % i for i in specIds]

        self.ccds = ['ccd_%s' % cam for cam in allcams]
        self.cam2ccd = dict([(cam, ccd) for cam, ccd in zip(allcams, self.ccds)])

        self.enus = ['enu_sm%i' % i for i in specIds]

        actorcore.ICC.ICC.__init__(self,
                                   name,
                                   productName=productName,
                                   configFile=configFile,
                                   modelNames=['seqno'] + self.ccds + self.enus)

        self.logger.setLevel(logLevel)

        self.everConnected = False

        self.doStop = False

    @property
    def cams(self):
        return self.config.get('sps', 'cams').split(',')

    def safeCall(self, doRaise=True, doRetry=False, **kwargs):
        cmd = kwargs["forUserCmd"]
        kwargs["timeLim"] = 300 if "timeLim" not in kwargs.keys() else kwargs["timeLim"]

        cmdVar = self.cmdr.call(**kwargs)

        if cmdVar.didFail and doRaise:
            reply = cmdVar.replyList[-1]
            raise RuntimeError("actor=%s %s" % (reply.header.actor, reply.keywords.canonical(delimiter=';')))
        return cmdVar

    def getSeqno(self, cmd):
        cmdVar = self.cmdr.call(actor='seqno',
                                cmdStr='getVisit',
                                forUserCmd=cmd,
                                timeLim=10)

        if cmdVar.didFail or not cmdVar.isDone:
            raise ValueError('getVisit has failed')

        visit = cmdVar.lastReply.keywords['visit'].values[0]

        return int(visit)

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
