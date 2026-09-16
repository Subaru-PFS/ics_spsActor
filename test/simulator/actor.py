"""The actor an Exposure runs inside, with no hub behind it.

Builds the real SpsConfig from pfs_instdata so light sources and shutter sets are the ones
the summit uses, routes every command to a simulated device, and keeps the transcript the
assertions are written against.
"""

import logging
import os
import threading

import ics.utils.time as pfsTime
import yaml
from ics.utils.sps.config import SpecModule, SpsConfig

from .devices import Ccd, Enu, Lamps
from .mhs import Cmd, CmdVar, Model

INSTDATA = os.path.expanduser('~/devel/ics/pfs_instdata')


class CommandFailure(Exception):
    """Raised inside a device to fail the command the scenario asked to fail."""


class Sim(object):
    """A spsActor stand-in.

    Parameters
    ----------
    specNums : list of `int`
        spectrograph modules to configure.
    lightSource : `str`
        light source every module is connected to.
    site : `str`
        pfs_instdata site section to read the module description from.
    """

    def __init__(self, specNums=(1,), lightSource='pfi', site='S'):
        self.bcast = Cmd('bcast')
        self.logger = logging.getLogger('sim.sps')
        self.models = dict()
        self.devices = dict()
        self.transcript = []
        self.inserted = []
        self.failures = dict()
        self.hooks = []
        self.lock = threading.Lock()
        self.startedAt = pfsTime.timestamp()

        with open(os.path.join(INSTDATA, 'config/actors/sps.yaml')) as cfg:
            self.actorConfig = yaml.safe_load(cfg)['sps']

        spsData = SpsData(lightSource)
        localConfig = self.actorConfig[site]
        self.spsConfig = SpsConfig([SpecModule.fromConfig(f'sm{specNum}', localConfig, spsData)
                                    for specNum in specNums])

        for specNum in specNums:
            specModule = self.spsConfig[f'sm{specNum}']
            self.attach(Enu(self, specModule.specName))

            for cam in specModule.cams.values():
                if cam.arm != 'n':
                    self.attach(Ccd(self, cam))

            if specModule.lightSource.lampsActor:
                self.attach(Lamps(self, specModule.lightSource.lampsActor))

        self.attach(Lamps(self, 'iis'))

    def attach(self, device):
        self.devices.setdefault(device.name, device)

    def declareModel(self, name):
        self.models.setdefault(name, Model())
        return self.models[name]

    def lamps(self, name):
        return self.devices[name]

    def prepareLamps(self, name, **onTimes):
        """Prepare a lamp run, as the iic sequence does before handing over the exposure."""
        cmdStr = ' '.join([f'{lamp}={onTime}' for lamp, onTime in onTimes.items()])
        return self.crudeCall(self.bcast, actor=name, cmdStr=f'prepare {cmdStr}')

    # -- the interface spsActor.main.SpsActor provides ------------------------------------

    def crudeCall(self, cmd, actor, cmdStr, timeLim=60, **kwargs):
        cmdStr = cmdStr.strip()

        with self.lock:
            self.transcript.append((round(pfsTime.timestamp() - self.startedAt, 3), actor, cmdStr))

        for hook in list(self.hooks):
            hook(self, actor, cmdStr)

        try:
            return self.devices[actor].call(cmdStr, timeLim)
        except CommandFailure as e:
            return CmdVar.failure(str(e), timeLim=timeLim)
        except KeyError:
            return CmdVar()

    def safeCall(self, cmd, actor, cmdStr, timeLim=60, **kwargs):
        cmdVar = self.crudeCall(cmd, actor, cmdStr, timeLim=timeLim, **kwargs)

        if cmdVar.didFail:
            cmd.warn(f'text="{actor} {cmdStr.split(" ")[0]} failed"')

        return cmdVar

    def insert(self, table, cmd=None, **kwargs):
        self.inserted.append((table, kwargs))

    # -- scenario support ------------------------------------------------------------------

    def failCommand(self, actor, at, reason=None):
        """Declare that actor fails at the given point, named by the device raising it."""
        self.failures[(actor, at)] = reason if reason else f'{actor} {at} failed on purpose'

    def failIfRequested(self, actor, at):
        """Raise if the scenario asked this point to fail, once."""
        reason = self.failures.pop((actor, at), None)

        if reason is not None:
            raise CommandFailure(reason)

    def onCommand(self, actor, cmdHead, func):
        """Call func() the first time actor is sent cmdHead.

        It runs in the thread that sent the command, just before the device sees it, so an
        injection lands at a known point of the exposure rather than at a plausible one.
        """
        fired = []

        def hook(sim, sentTo, cmdStr):
            if fired or sentTo != actor or not cmdStr.startswith(cmdHead):
                return

            fired.append(True)
            func()

        self.hooks.append(hook)

    def waitForQuiet(self, idle=0.25, timeout=10):
        """Wait until no command has been sent for idle seconds.

        Some of what an exposure does on its way out is asynchronous, a stop released in its
        own thread most of all, so the transcript is only complete once it stops growing.
        """
        deadline = pfsTime.timestamp() + timeout

        while pfsTime.timestamp() < deadline:
            sent = len(self.transcript)
            pfsTime.sleep.millisec(int(idle * 1000))

            if len(self.transcript) == sent:
                return True

        return False

    def onKeyVar(self, actor, key, value, func):
        """Call func() the first time that keyVar takes the given value.

        Nothing is sent while the shutters are open, so a command hook cannot reach the
        middle of an integration; a keyword transition can.
        """
        fired = []

        def callback(keyVar):
            if fired or keyVar.getValue(doRaise=False) != value:
                return

            fired.append(True)
            func()

        self.models[actor].keyVarDict[key].watch(callback)

    def sent(self, actor=None, cmdHead=None):
        """Return the commands sent, filtered by actor and by command head."""
        return [(at, sentTo, cmdStr) for at, sentTo, cmdStr in self.transcript
                if (actor is None or sentTo == actor) and (cmdHead is None or cmdStr.startswith(cmdHead))]

    def printTranscript(self):
        for at, actor, cmdStr in self.transcript:
            print(f'    {at:7.3f}  {actor:10s} {cmdStr}')


class SpsData(object):
    """Stands in for the instdata store SpecModule reads its light source from."""

    def __init__(self, lightSource):
        self.lightSource = lightSource

    def loadKey(self, key):
        return [self.lightSource]
