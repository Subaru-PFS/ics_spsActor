import logging

from spsActor.Controllers.exposure import Exposure
from spsActor.utils import wait


class expose(object):
    def __init__(self, actor, name, loglevel=logging.DEBUG):
        """This sets up the connections to/from the hub, the logger, and the twisted reactor.

        :param actor: spsaitActor
        :param name: controller name
        """
        self.actor = actor
        self.name = name
        self.doStop = False

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)

    def resetExposure(self):
        """ reset exposure stop flag """

        self.doStop = False

    def stopExposure(self, cmd):
        """ activate exposure stop flag, call enu exposure abort function"""
        self.doStop = True

        for enu in self.actor.enus:
            self.actor.safeCall(actor=enu, cmdStr='exposure abort', forUserCmd=cmd, timeLim=10)

    def expose(self, cmd, exptype, exptime, visit, cams):
        """ create Exposure object wait for threaded jobs to be finished

        raise RuntimeError if not a single CamExposure file has been created
        finally: free up all ressources
        """
        exp = Exposure(self.actor, exptype, exptime, cams)

        try:
            exp.start(cmd, visit)

            while exp.notFinished:
                wait()

            if not exp.isIdle:
                msg = 'Exposure aborted' if self.doStop else 'Exposure has failed'
                raise RuntimeError(msg)

            exp.store(visit)

        finally:
            exp.exit()

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass
