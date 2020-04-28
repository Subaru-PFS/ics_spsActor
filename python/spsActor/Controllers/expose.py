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
        self.current = None

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)

    def expose(self, cmd, exptype, exptime, visit, cams):
        """ create Exposure object wait for threaded jobs to be finished

        raise RuntimeError if not a single CamExposure file has been created
        finally: free up all ressources
        """
        self.current = Exposure(self.actor, exptype, exptime, cams)

        try:
            self.current.start(cmd, visit)

            while not self.current.isFinished:
                wait()

            if not self.current.isIdle:
                msg = 'Exposure aborted' if self.current.doAbort else 'Exposure has failed'
                raise RuntimeError(msg)

            self.current.store(cmd, visit)

        finally:
            self.current.exit()
            self.current = None

    def start(self, *args, **kwargs):
        pass

    def stop(self, *args, **kwargs):
        pass
