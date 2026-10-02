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
    """Base device, dispatching a command string to a method named after its first word.

    A serialized device runs one command at a time, as an actor whose handlers block does:
    actorcore takes commands one by one, so a command sent while a slow one runs waits for
    it. Every command's start and end are kept in `executed`.
    """

    def __init__(self, sim, name, serialized=False):
        self.sim = sim
        self.name = name
        self.model = sim.declareModel(name)
        self.serialized = serialized
        self.cmdLock = threading.Lock()
        self.executed = []

    def key(self, name):
        return self.model.keyVarDict[name]

    def call(self, cmdStr, timeLim):
        """Dispatch cmdStr, returning the cmdVar the caller gets back."""
        if not self.serialized:
            return self.dispatch(cmdStr, timeLim)

        with self.cmdLock:
            return self.dispatch(cmdStr, timeLim)

    def dispatch(self, cmdStr, timeLim):
        head, __, args = cmdStr.partition(' ')
        func = getattr(self, f'do_{head}', None)
        startedAt = pfsTime.timestamp()

        try:
            return func(self.parseArgs(args), timeLim) if func is not None else CmdVar()
        finally:
            self.executed.append((head, startedAt, pfsTime.timestamp()))

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
        self.model.declare('slitPosition', 'home')
        self.model.declare('slitAtSpeed', False)
        self.finishNow = threading.Event()
        self.closedAt = None

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

        closedAt = self.closedAt = pfsTime.timestamp()
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
    """A b/r/m detector, announcing each state it goes through and its readout progress."""

    wipeTime = 0.05
    ROWS, EVERY = 4300, 500

    def __init__(self, sim, cam, readTime=0.05):
        Device.__init__(self, sim, f'ccd_{cam}')
        self.cam = cam
        self.readTime = readTime
        self.rowsAt = []
        self.model.declare('exposureState', 'idle')
        self.model.declare('readRows', [0, Ccd.ROWS])

    def do_wipe(self, args, timeLim):
        self.sim.failIfRequested(self.name, 'wipe')
        self.key('exposureState').set('wiping')
        pfsTime.sleep.millisec(int(Ccd.wipeTime * 1000))
        self.key('exposureState').set('integrating')
        return CmdVar()

    def do_read(self, args, timeLim):
        """Read out, publishing readRows every EVERY rows as the ccd actor does."""
        self.sim.failIfRequested(self.name, 'read')
        self.key('exposureState').set('reading')

        steps = list(range(0, Ccd.ROWS, Ccd.EVERY)) + [Ccd.ROWS - 1]
        for rows in steps:
            pfsTime.sleep.millisec(int(self.readTime * 1000 / len(steps)))
            self.rowsAt.append((pfsTime.timestamp(), rows / Ccd.ROWS))
            self.key('readRows').set([rows, Ccd.ROWS])

        self.key('exposureState').set('idle')

        armNum = dict(b=1, r=2, n=3, m=4)[self.cam.arm]
        return CmdVar(keywords=[('beamConfigDate', [int(args['visit']), 59000.0]),
                                ('spsFileIds', [str(self.cam), '2026-09-16', int(args['visit']),
                                                self.cam.specNum, armNum])])

    def do_clearExposure(self, args, timeLim):
        self.key('exposureState').set('idle')
        return CmdVar()


class Hx(Device):
    """An H4 detector: a free-running ramp of reads that can only be told when to stop.

    Records when each read ended and when it was told to finish, which is what the ramp
    assertions read back.
    """

    def __init__(self, sim, cam, readTime, irpRatio, startupTime=0.05):
        Device.__init__(self, sim, f'hx_{cam}')
        self.cam = cam
        self.readTime = readTime
        self.startupTime = startupTime
        self.readsAt = []
        self.finishes = []
        self.nread = None
        self.stopAfter = None
        self.model.declare('readTime', readTime)
        self.model.declare('irp', [bool(irpRatio), irpRatio, irpRatio])
        self.model.declare('hxread')
        self.model.declare('filename')

    def do_ramp(self, args, timeLim):
        if args.pop('finish', False):
            return self.finishRamp(args)

        visit, self.nread = int(args['visit']), int(args['nread'])
        self.readsAt, self.finishes, self.stopAfter = [], [], None

        pfsTime.sleep.millisec(int(self.startupTime * 1000))
        pfsTime.sleep.millisec(int(self.readTime * 1000))
        self.key('hxread').set([visit, 1, 0, 1])

        for read in range(1, self.nread + 1):
            pfsTime.sleep.millisec(int(self.readTime * 1000))
            self.readsAt.append(pfsTime.timestamp())
            self.key('hxread').set([visit, 1, 1, read])

            if self.stopAfter is not None and read >= self.stopAfter:
                break

        self.key('filename').set(f'/data/raw/2026-09-24/ramps/PFSB{visit:06d}{self.cam.specNum}3.fits')
        return CmdVar()

    def finishRamp(self, args):
        """ramp finish [stopRamp]: with stopRamp, the read after the current one is the last."""
        self.finishes.append((pfsTime.timestamp(), bool(args.get('stopRamp'))))
        if args.get('stopRamp'):
            self.stopAfter = len(self.readsAt) + 1

        return CmdVar()


class Lamps(Device):
    """A lamp controller: pfilamps, dcb or iis.

    Records whether its lamps were fired and whether they were released, which is what the
    illuminator assertions read back.
    """

    warmupTime = 0.05

    def __init__(self, sim, name, goLatency=0, serialized=False):
        Device.__init__(self, sim, name, serialized=serialized)
        self.prepared = dict()
        self.wentGo = False
        self.stopped = 0
        self.goLatency = goLatency
        self.litAt = self.offAt = None
        self.cutShort = threading.Event()

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

        pfsTime.sleep.millisec(int(self.goLatency * 1000))
        self.litAt = pfsTime.timestamp()
        for lamp in self.prepared:
            self.key(lamp).set(['on', isoNow(), isoNow()])

        if args.pop('noWait', False):
            return CmdVar()

        # a stop reaching the controller mid-pulse cuts it short; a serialized one never can.
        self.cutShort.clear()
        self.cutShort.wait(timeout=onTime)

        self.offAt = pfsTime.timestamp()
        for lamp in self.prepared:
            self.key(lamp).set(['off', isoNow(), isoNow()])

        return CmdVar()

    def do_stop(self, args, timeLim):
        """Release the lamps, the only thing that clears what prepare declared."""
        self.stopped += 1
        self.cutShort.set()

        for lamp in self.prepared:
            self.key(lamp).set(['off', isoNow(), isoNow()])

        self.prepared = dict()
        return CmdVar()
