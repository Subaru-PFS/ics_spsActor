import logging
import time

from actorcore.QThread import QThread
from spsActor.utils import Exposure

class expose(QThread):
    def __init__(self, actor, name, loglevel=logging.DEBUG):
        """This sets up the connections to/from the hub, the logger, and the twisted reactor.

        :param actor: spsaitActor
        :param name: controller name
        """
        QThread.__init__(self, actor, name, timeout=2)
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)

    def expose(self, cmd, exptype, exptime, cams):
        cams = cams if cams else self.actor.cams
        visit = self.actor.getSeqno(cmd=cmd)
        exposure = Exposure(actor=self.actor,
                            cams=cams,
                            visit=visit,
                            exptype=exptype,
                            exptime=exptime,
                            cmd=cmd)

        exposure.wipeCcd(cmd=cmd)
        exposure.waitAndHandle(state='integrating', timeout=90)

        exposure.cmdShutters(cmd=cmd, exptime=exptime)

        exposure.waitAndHandle(state='reading', timeout=60 + exptime)
        exposure.waitAndHandle(state='idle', timeout=180, force=True)

        start = time.time()
        while not exposure.filesExist():
            if time.time() - start > exposure.timeout:
                raise Exception('no exposure has been created')

        visit = exposure.store()
        return visit

    def calibExposure(self, cmd, cams, exptype, exptime):
        cams = cams if cams else self.actor.cams
        visit = self.actor.getSeqno(cmd=cmd)
        exposure = Exposure(actor=self.actor,
                            cams=cams,
                            visit=visit,
                            exptype=exptype,
                            exptime=exptime,
                            cmd=cmd)

        exposure.wipeCcd(cmd=cmd)
        exposure.readCcd(cmd=cmd, exptime=exptime)

        exposure.waitAndHandle(state='reading', timeout=60 + exptime)
        exposure.waitAndHandle(state='idle', timeout=180, force=True)

        start = time.time()
        while not exposure.filesExist():
            if time.time() - start > exposure.timeout:
                raise Exception('no exposure has been created')

        visit = exposure.store()
        return visit

    def start(self, cmd=None):
        QThread.start(self)

    def handleTimeout(self):
        """| Is called when the thread is idle
        """
        pass
