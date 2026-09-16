"""Simulated devices answering the commands an exposure sends.

Each device owns the keyVars the real actor publishes and drives them on the same timeline:
shutters open and close around the integration, a ccd announces every state it passes
through, a lamp controller reports whether it is ready. The exposure under test sees the
keyword traffic it would see on the summit, at a scale of a second rather than a minute.
"""

import threading

import ics.utils.time as pfsTime

from .mhs import CmdVar


def isoNow(timestamp=None):
    """Return an iso-formatted date for the given timestamp, now by default."""
    timestamp = pfsTime.timestamp() if timestamp is None else timestamp
    return pfsTime.convert.datetime_to_isoformat(pfsTime.convert.datetime_from_timestamp(timestamp))


class Device(object):
    """Base device, dispatching a command string to a method named after its first word."""

    def __init__(self, sim, name):
        self.sim = sim
        self.name = name
        self.model = sim.declareModel(name)

    def key(self, name):
        return self.model.keyVarDict[name]

    def call(self, cmdStr, timeLim):
        """Dispatch cmdStr, returning the cmdVar the caller gets back."""
        head, __, args = cmdStr.partition(' ')
        func = getattr(self, f'do_{head}', None)

        if func is None:
            return CmdVar()

        return func(self.parseArgs(args), timeLim)

    @staticmethod
    def parseArgs(args):
        """Return {key: value} for the key=value tokens, and {flag: True} for the bare ones."""
        parsed = dict()

        for token in args.split():
            key, sep, value = token.partition('=')
            parsed[key] = value if sep else True

        return parsed


class Enu(Device):
    """Spectrograph front-end: the shutters, and the command that closes them early."""

    openingTime = 0.05

    def __init__(self, sim, specName):
        Device.__init__(self, sim, f'enu_{specName}')
        self.model.declare('shutters', 'none')
        self.model.declare('shutterTimings')
        self.model.declare('bia', 'off')
        self.model.declare('slitAtSpeed', False)
        self.finishNow = threading.Event()

    def do_shutters(self, args, timeLim):
        """Open the shutters, hold them for exptime unless finished early, then close."""
        if not args.pop('expose', False):
            return CmdVar()

        exptime, visit = float(args['exptime']), int(args['visit'])
        self.finishNow.clear()

        startedAt = pfsTime.timestamp()
        self.sim.failIfRequested(self.name, 'shutters expose, before opening')
        pfsTime.sleep.millisec(int(Enu.openingTime * 1000))

        openedAt = pfsTime.timestamp()
        self.key('shutters').set('open')
        self.sim.failIfRequested(self.name, 'shutters expose, after opening')

        # the enu closes the shutters when the exposure is finished early, hence the wait.
        self.finishNow.wait(timeout=exptime)

        closedAt = pfsTime.timestamp()
        self.key('shutters').set('close')
        self.key('shutterTimings').set([visit, isoNow(startedAt), isoNow(openedAt),
                                        isoNow(closedAt), isoNow(closedAt)])

        return CmdVar(keywords=[('exptime', [round(closedAt - openedAt, 3)]),
                                ('dateobs', [isoNow(openedAt)])])

    def do_exposure(self, args, timeLim):
        """Finish the on-going integration now."""
        if args.pop('finish', False):
            self.finishNow.set()

        return CmdVar()


class Ccd(Device):
    """A b/r/m detector, announcing each state it goes through."""

    wipeTime = readTime = 0.05

    def __init__(self, sim, cam):
        Device.__init__(self, sim, f'ccd_{cam}')
        self.cam = cam
        self.model.declare('exposureState', 'idle')

    def do_wipe(self, args, timeLim):
        self.sim.failIfRequested(self.name, 'wipe')
        self.key('exposureState').set('wiping')
        pfsTime.sleep.millisec(int(Ccd.wipeTime * 1000))
        self.key('exposureState').set('integrating')
        return CmdVar()

    def do_read(self, args, timeLim):
        self.sim.failIfRequested(self.name, 'read')
        self.key('exposureState').set('reading')
        pfsTime.sleep.millisec(int(Ccd.readTime * 1000))
        self.key('exposureState').set('idle')

        armNum = dict(b=1, r=2, n=3, m=4)[self.cam.arm]
        return CmdVar(keywords=[('beamConfigDate', [int(args['visit']), 59000.0]),
                                ('spsFileIds', [str(self.cam), '2026-09-16', int(args['visit']),
                                                self.cam.specNum, armNum])])

    def do_clearExposure(self, args, timeLim):
        self.key('exposureState').set('idle')
        return CmdVar()


class Lamps(Device):
    """A lamp controller: pfilamps, dcb or iis.

    Records whether its lamps were fired and whether they were released, which is what the
    illuminator assertions read back.
    """

    warmupTime = 0.05

    def __init__(self, sim, name):
        Device.__init__(self, sim, name)
        self.prepared = dict()
        self.wentGo = False
        self.stopped = 0

        for lamp in ('halogen', 'neon', 'argon', 'krypton', 'xenon', 'hgar', 'hgcd'):
            self.model.declare(lamp, ['off', isoNow(), isoNow()])

    @property
    def isLit(self):
        """A lamp run was started and never released."""
        return self.wentGo and not self.stopped

    def do_prepare(self, args, timeLim):
        self.prepared = dict([(lamp, float(onTime)) for lamp, onTime in args.items()])
        return CmdVar()

    def do_waitForReadySignal(self, args, timeLim):
        self.sim.failIfRequested(self.name, 'waitForReadySignal')
        pfsTime.sleep.millisec(int(Lamps.warmupTime * 1000))
        return CmdVar()

    def do_go(self, args, timeLim):
        """Fire the prepared lamps, blocking for their on-time unless noWait is given."""
        self.sim.failIfRequested(self.name, 'go')
        self.wentGo = True
        onTime = max(self.prepared.values()) if self.prepared else 0

        for lamp in self.prepared:
            self.key(lamp).set(['on', isoNow(), isoNow()])

        if args.pop('noWait', False):
            return CmdVar()

        pfsTime.sleep.millisec(int(onTime * 1000))

        for lamp in self.prepared:
            self.key(lamp).set(['off', isoNow(), isoNow()])

        return CmdVar()

    def do_stop(self, args, timeLim):
        """Release the lamps, the only thing that clears what prepare declared."""
        self.stopped += 1

        for lamp in self.prepared:
            self.key(lamp).set(['off', isoNow(), isoNow()])

        self.prepared = dict()
        return CmdVar()
