"""Scenarios driving spsActor through its own expose vocabulary.

Each scenario builds a simulated spectrograph, sends one `sps expose` command, and checks
what the actor did: which files came back, and above all which illuminators were released
and which were left burning. Failures, aborts and early finishes are injected at named
points of the exposure, so the timing of an interruption is part of the scenario rather
than a matter of luck.
"""

import ics.utils.time as pfsTime
from spsActor.Commands.ExposeCmd import ExposeCmd

from simulator.actor import Sim
from simulator.command import CmdSet

EXPTIME = 0.4


class Result(object):
    """What a scenario has to say about the exposure it ran."""

    def __init__(self, sim, cmd):
        self.sim = sim
        self.cmd = cmd

    @property
    def fileIds(self):
        replies = [text for text in self.cmd.says('F') + self.cmd.says('w') if 'fileIds' in text]
        return replies[0] if replies else None

    def stopped(self, lampsActor):
        """How many times that illuminator was released."""
        return self.sim.lamps(lampsActor).stopped

    def isLit(self, lampsActor):
        """Whether a lamp run was started and never released."""
        return self.sim.lamps(lampsActor).isLit

    def sent(self, actor=None, cmdHead=None):
        return [cmdStr for __, __, cmdStr in self.sim.sent(actor=actor, cmdHead=cmdHead)]

    def firstAt(self, actor, cmdHead):
        """Timestamp of the first matching command, None if it was never sent."""
        matched = self.sim.sent(actor=actor, cmdHead=cmdHead)
        return matched[0][0] if matched else None


def expose(cmdStr, specNums=(1,), lightSource='pfi', prepare=None, inject=None, timeout=60):
    """Run one exposure against a fresh simulated spectrograph.

    Parameters
    ----------
    cmdStr : `str`
        the `sps expose ...` command to send.
    prepare : `dict`
        lamp on-times to prepare beforehand, keyed by lamp actor.
    inject : callable
        called with (sim, cmdSet) before the exposure starts, to arm an injection.
    """
    sim = Sim(specNums=specNums, lightSource=lightSource)
    cmdSet = CmdSet(sim, ExposeCmd)

    for lampsActor, onTimes in (prepare or dict()).items():
        sim.prepareLamps(lampsActor, **onTimes)

    if inject is not None:
        inject(sim, cmdSet)

    cmd = cmdSet.call(cmdStr)
    deadline = pfsTime.timestamp() + timeout

    while not cmd.isDone:
        if pfsTime.timestamp() > deadline:
            raise TimeoutError(f'{cmdStr!r} never concluded, transcript:\n{sim.transcript}')

        pfsTime.sleep.millisec()

    sim.waitForQuiet()

    return Result(sim, cmd)


def abortAt(actor, cmdHead, visit=1):
    """Arm an `sps exposure abort` to land just before actor is sent cmdHead."""

    def inject(sim, cmdSet):
        sim.onCommand(actor, cmdHead, lambda: cmdSet.call(f'exposure abort visit={visit}'))

    return inject


def finishAt(actor, cmdHead, visit=1):
    """Arm an `sps exposure finish` to land just before actor is sent cmdHead."""

    def inject(sim, cmdSet):
        sim.onCommand(actor, cmdHead, lambda: cmdSet.call(f'exposure finish visit={visit}'))

    return inject


def knownGap(reason):
    """Mark a scenario that describes behaviour the actor does not have yet.

    The runner expects it to fail, and says so loudly if it starts passing.
    """

    def decorate(func):
        func.knownGap = reason
        return func

    return decorate


def whileIntegrating(what, specName='sm1', visit=1):
    """Arm an `sps exposure abort|finish` to land while the shutters are open."""

    def inject(sim, cmdSet):
        sim.onKeyVar(f'enu_{specName}', 'shutters', 'open',
                     lambda: cmdSet.call(f'exposure {what} visit={visit}'))

    return inject


def failAt(actor, at):
    """Arm a device failure at a named point."""

    def inject(sim, cmdSet):
        sim.failCommand(actor, at)

    return inject


# -- happy path ----------------------------------------------------------------------------

def test_bias_takes_no_illuminator():
    res = expose('expose bias cams=b1 visit=1')
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 0
    assert res.sent('enu_sm1', 'shutters') == [], 'a bias opened the shutters'


def test_dark_takes_no_illuminator():
    res = expose(f'expose dark exptime={EXPTIME} cams=b1 visit=1')
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 0


def test_object_without_lamps_stops_nothing():
    res = expose(f'expose object exptime={EXPTIME} cams=b1 visit=1 isLast')
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 0, 'stopped a lamp that was never fired'
    assert res.stopped('iis') == 0


def test_iis_is_released_on_the_last_exposure():
    res = expose(f'expose object exptime={EXPTIME} cams=b1 visit=1 doIIS isLast',
                 prepare=dict(iis=dict(halogen=0.1)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('iis') == 1, 'iis was not released'
    assert not res.isLit('iis')


def test_iis_is_left_alone_before_the_last_exposure():
    res = expose(f'expose object exptime={EXPTIME} cams=b1 visit=1 doIIS',
                 prepare=dict(iis=dict(halogen=0.1)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('iis') == 0, 'iis released before its run ended'


def test_lamps_released_once_after_shutters_close():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 1, f'released {res.stopped("pfilamps")} times, expected once'
    assert res.firstAt('pfilamps', 'stop') > res.firstAt('enu_sm1', 'exposure finish')


def test_lamps_left_alone_before_the_last_exposure():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 0, 'released before its run ended'


def test_shutter_timed_lamps_are_cut_every_exposure():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doShutterTiming',
                 prepare=dict(pfilamps=dict(hgcd=30)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 1, 'a lamp outliving the shutters was left burning'


def test_backgrounded_lamps_survive_until_the_last_exposure():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 bckIlluminators=pfilamps',
                 prepare=dict(pfilamps=dict(hgcd=30)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 0, 'cut a backgrounded run short'


def test_backgrounded_lamps_released_on_the_last_exposure():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 bckIlluminators=pfilamps isLast',
                 prepare=dict(pfilamps=dict(hgcd=30)))
    assert res.fileIds, 'no file produced'
    assert res.stopped('pfilamps') == 1, 'backgrounded run never released'


def test_mixed_runs_end_independently():
    """iis is pulsed per exposure while pfilamps is backgrounded across the sequence."""
    notLast = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doIIS bckIlluminators=pfilamps',
                     prepare=dict(pfilamps=dict(hgcd=30), iis=dict(halogen=0.1)))
    assert notLast.stopped('pfilamps') == 0, 'cut the backgrounded run short'

    last = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doIIS bckIlluminators=pfilamps isLast',
                  prepare=dict(pfilamps=dict(hgcd=30), iis=dict(halogen=0.1)))
    assert last.stopped('pfilamps') == 1, 'backgrounded run never released'
    assert last.stopped('iis') == 1, 'iis never released'


def test_two_spectrographs_stay_synchronised():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1,b2 visit=1 doLamps isLast', specNums=(1, 2),
                 prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert res.fileIds, 'no file produced'
    assert 'b1' in res.fileIds and 'b2' in res.fileIds, res.fileIds
    assert res.stopped('pfilamps') == 1, 'released once per module instead of once per exposure'


# -- injected failures ---------------------------------------------------------------------

def test_wipe_failure_stops_nothing_since_go_never_went():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)), inject=failAt('ccd_b1', 'wipe'))
    assert res.cmd.didFail, 'a failed wipe did not fail the command'
    assert res.sent('ccd_b1', 'clearExposure'), 'detector was not cleared'
    assert res.stopped('pfilamps') == 0, 'released a lamp that was never fired'


def test_go_failure_releases_the_lamps():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)), inject=failAt('pfilamps', 'go'))
    assert res.cmd.didFail, 'a failed go did not fail the command'
    assert res.stopped('pfilamps') == 1, 'lamps left burning after a failed go'


def test_shutter_failure_before_opening_discards_the_data():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)),
                 inject=failAt('enu_sm1', 'shutters expose, before opening'))
    assert res.cmd.didFail, 'a failed shutter did not fail the command'
    assert res.sent('ccd_b1', 'clearExposure'), 'detector was not cleared'


def test_shutter_failure_after_opening_keeps_the_data():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)),
                 inject=failAt('enu_sm1', 'shutters expose, after opening'))
    assert res.sent('ccd_b1', 'read'), 'data was discarded although the shutters had opened'
    assert res.cmd.didFail, 'the shutter failure was not reported'


@knownGap('the enu leaves the shutters open when it fails mid-exposure, so no close ever comes, '
          'and reading the data is not an abort either: nothing ends the illuminator run')
def test_shutter_failure_after_opening_releases_the_lamps():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)),
                 inject=failAt('enu_sm1', 'shutters expose, after opening'))
    assert res.stopped('pfilamps') == 1, 'lamps left burning'


def test_read_failure_is_reported_but_keeps_the_lamps_honest():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)), inject=failAt('ccd_b1', 'read'))
    assert res.cmd.didFail, 'a failed read did not fail the command'
    assert res.stopped('pfilamps') == 1, 'lamps left burning after a failed read'


# -- injected abort and finish ---------------------------------------------------------------

def test_abort_during_wipe_stops_nothing_since_go_never_went():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doLamps isLast',
                 prepare=dict(pfilamps=dict(neon=EXPTIME)), inject=abortAt('ccd_b1', 'wipe'))
    assert res.stopped('pfilamps') == 0, 'released a lamp that was never fired'
    assert res.sent('ccd_b1', 'clearExposure'), 'detector was not cleared'


def test_abort_before_shutters_open_still_releases_a_lit_lamp():
    """doShutterTiming fires the lamps before the shutters, so there is something to release
    even though no close will ever come."""
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doShutterTiming',
                 prepare=dict(pfilamps=dict(hgcd=30)), inject=abortAt('enu_sm1', 'shutters expose'))
    assert res.stopped('pfilamps') == 1, 'lamps left burning, no shutter close ever came'
    assert not res.isLit('pfilamps')


def test_abort_of_a_backgrounded_run_releases_it():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 bckIlluminators=pfilamps',
                 prepare=dict(pfilamps=dict(hgcd=30)), inject=abortAt('ccd_b1', 'wipe'))
    assert res.stopped('pfilamps') == 1, 'backgrounded run left burning after an abort'


def test_finish_during_integration_cuts_it_short_and_keeps_the_data():
    res = expose('expose object exptime=20 cams=b1 visit=1 isLast',
                 inject=whileIntegrating('finish'))
    assert res.sent('enu_sm1', 'exposure finish'), 'the shutters were never told to close'
    assert res.fileIds, 'early finish discarded the data'

    [read] = res.sent('ccd_b1', 'read')
    exptime = float(read.split('exptime=')[1].split()[0])
    assert exptime < 5, f'shutters stayed open for {exptime}s of a 20s exposure'


def test_abort_during_integration_discards_nothing_already_exposed():
    res = expose('expose object exptime=20 cams=b1 visit=1 isLast',
                 inject=whileIntegrating('abort'))
    assert res.sent('enu_sm1', 'exposure finish'), 'the shutters were never told to close'
    assert res.sent('ccd_b1', 'read'), 'photons had landed, yet the data was discarded'


def test_finishing_a_backgrounded_run_releases_it():
    """An exposure finished by hand or by the sequence is the end of that run: iic concludes
    the sequence on a finishNow, so nothing later will use the lamps."""
    res = expose('expose arc exptime=20 cams=b1 visit=1 bckIlluminators=pfilamps',
                 prepare=dict(pfilamps=dict(hgcd=30)), inject=whileIntegrating('finish'))
    assert res.stopped('pfilamps') == 1, 'backgrounded run left burning after an early finish'


def test_abort_during_integration_releases_the_lamps():
    res = expose(f'expose arc exptime={EXPTIME} cams=b1 visit=1 doShutterTiming',
                 prepare=dict(pfilamps=dict(hgcd=30)), inject=abortAt('enu_sm1', 'exposure finish'))
    assert res.stopped('pfilamps') == 1, 'lamps left burning after an abort'
