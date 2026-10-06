"""Scenarios driving `sps flash`: shutters opened for a lamp pulse, with no detector and no visit."""

import ics.utils.time as pfsTime

from testExposure import EXPTIME, expose, failAt


def flashEndOnOpen(what, specName='sm1'):
    """Arm an `sps flash abort|finish` to land as soon as the shutters are open."""

    def inject(sim, cmdSet):
        sim.onKeyVar(f'enu_{specName}', 'shutters', 'open', lambda: cmdSet.call(f'flash {what}'))

    return inject


def detectorCommands(res):
    return [cmdStr for __, actor, cmdStr in res.sim.transcript if actor.startswith(('ccd_', 'hx_'))]


# -- happy path ----------------------------------------------------------------------------

def test_flash_pulses_the_lamps_through_open_shutters():
    res = expose(f'flash exptime={EXPTIME} cams=b1,r1,n1 doLamps', prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert not res.cmd.didFail, f'flash failed: {res.cmd.says("f")}'
    assert not detectorCommands(res), f'detectors were driven: {detectorCommands(res)}'
    assert res.firstAt('pfilamps', 'waitForReadySignal') < res.firstAt('enu_sm1', 'shutters expose')
    assert res.firstAt('enu_sm1', 'shutters expose') < res.firstAt('pfilamps', 'go'), 'lamps fired before the shutters'
    assert res.firstAt('pfilamps', 'go') < res.firstAt('enu_sm1', 'exposure finish'), 'shutters closed before the pulse'


def test_flash_credits_no_visit():
    res = expose(f'flash exptime={EXPTIME} cams=b1 doLamps', prepare=dict(pfilamps=dict(neon=EXPTIME)))
    [shutters] = res.sent('enu_sm1', 'shutters expose')
    assert 'visit=' not in shutters, f'a visit was sent to the shutters: {shutters}'
    assert not res.sim.inserted, f'a flash wrote to opdb: {res.sim.inserted}'
    assert not res.fileIds, 'a flash declared files'


def test_flash_opens_only_the_selected_arm_shutters():
    """The red shutter sits before the dichroic, so blue needs both and red only its own."""
    red = expose(f'flash exptime={EXPTIME} cams=r1 doLamps', prepare=dict(pfilamps=dict(neon=EXPTIME)))
    blue = expose(f'flash exptime={EXPTIME} cams=b1 doLamps', prepare=dict(pfilamps=dict(neon=EXPTIME)))
    [redCmd] = red.sent('enu_sm1', 'shutters expose')
    [blueCmd] = blue.sent('enu_sm1', 'shutters expose')
    assert redCmd.split('shutterMask=')[1] != blueCmd.split('shutterMask=')[1], 'arms do not select the shutters'


def test_flash_with_iis_leaves_pfilamps_alone():
    res = expose(f'flash exptime={EXPTIME} cams=b1 doIIS', prepare=dict(iis=dict(hgar=EXPTIME)))
    assert not res.cmd.didFail, f'flash failed: {res.cmd.says("f")}'
    assert res.sent('iis', 'go'), 'iis never fired'
    assert not res.sent('pfilamps'), f'pfilamps was driven: {res.sent("pfilamps")}'
    assert res.firstAt('enu_sm1', 'shutters expose') < res.firstAt('iis', 'go'), 'iis fired before the shutters'


def test_flash_opens_the_shutters_once_iis_is_warm():
    """The iis warm-up, 60 s for hgar, has to hold the shutters as the main lamps' does."""
    res = expose(f'flash exptime={EXPTIME} cams=b1 doIIS', prepare=dict(iis=dict(hgar=EXPTIME)), iisWarmup=1)
    assert not res.cmd.didFail, f'flash failed: {res.cmd.says("f")}'
    [warmedAt] = [end for head, __, end in res.sim.devices['iis'].executed if head == 'waitForReadySignal']
    assert res.firstAt('enu_sm1', 'shutters expose') + res.sim.startedAt >= warmedAt, 'shutters opened on a cold lamp'
    assert res.sent('iis', 'go'), 'iis never fired'


def test_flash_waits_for_every_module_before_firing():
    res = expose(f'flash exptime={EXPTIME} specNums=1,2 doLamps', specNums=(1, 2),
                 prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert not res.cmd.didFail, f'flash failed: {res.cmd.says("f")}'
    assert len(res.sent('pfilamps', 'go')) == 1, 'lamps fired more than once'
    assert res.sim.devices['enu_sm1'].closedAt and res.sim.devices['enu_sm2'].closedAt, 'a module never closed'


# -- lamp run ------------------------------------------------------------------------------

def test_flash_leaves_the_lamps_alone_before_the_last():
    res = expose(f'flash exptime={EXPTIME} cams=b1 doLamps', prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert res.stopped('pfilamps') == 0, 'released before its run ended'


def test_flash_releases_the_lamps_on_the_last():
    res = expose(f'flash exptime={EXPTIME} cams=b1 doLamps isLast', prepare=dict(pfilamps=dict(neon=EXPTIME)))
    assert res.stopped('pfilamps') == 1, f'released {res.stopped("pfilamps")} times, expected once'


# -- refused -------------------------------------------------------------------------------

def test_flash_needs_exactly_one_illuminator():
    for flags in ('', 'doLamps doIIS'):
        res = expose(f'flash exptime={EXPTIME} cams=b1 {flags}')
        assert res.cmd.didFail, f'flash with {flags!r} was accepted'
        assert not res.sent('enu_sm1'), 'shutters moved for a refused flash'


def test_a_second_flash_is_refused_while_one_runs():
    second = []

    def inject(sim, cmdSet):
        sim.onKeyVar('enu_sm1', 'shutters', 'open',
                     lambda: second.append(cmdSet.call(f'flash exptime={EXPTIME} cams=b1 doLamps')))

    res = expose(f'flash exptime={EXPTIME} cams=b1 doLamps', prepare=dict(pfilamps=dict(neon=EXPTIME)),
                 inject=inject)
    [cmd] = second
    deadline = pfsTime.timestamp() + 5
    while not cmd.isDone and pfsTime.timestamp() < deadline:
        pfsTime.sleep.millisec()

    assert cmd.didFail, 'a second flash ran alongside the first'
    assert not res.cmd.didFail, 'the second flash disturbed the first'
    assert len(res.sent('enu_sm1', 'shutters expose')) == 1, 'shutters opened twice'


# -- interrupted ---------------------------------------------------------------------------

def test_flash_finish_closes_the_shutters_and_succeeds():
    def inject(sim, cmdSet):
        sim.onCommand('iis', 'go', lambda: cmdSet.call('flash finish'))

    res = expose('flash exptime=20 cams=b1 doIIS', prepare=dict(iis=dict(hgar=20)), inject=inject)
    assert not res.cmd.didFail, f'finished flash reported a failure: {res.cmd.says("f")}'
    assert res.sent('enu_sm1', 'exposure finish'), 'the shutters were never told to close'
    assert res.stopped('iis') == 1, 'iis left burning after the flash was ended'
    shutters = res.sim.devices['enu_sm1']
    assert shutters.closedAt - res.firstAt('enu_sm1', 'shutters expose') - res.sim.startedAt < 5, \
        'shutters stayed open for the whole flash'


def test_flash_finish_before_the_lamps_fired_is_an_abort():
    """Nothing was flashed, so the flash cannot report success."""
    res = expose('flash exptime=20 cams=b1 doIIS', prepare=dict(iis=dict(hgar=20)),
                 inject=flashEndOnOpen('finish'))
    assert res.cmd.didFail, 'a flash that never fired reported success'
    assert res.sent('enu_sm1', 'exposure finish'), 'the shutters were never told to close'


def test_flash_abort_closes_the_shutters_and_fails():
    res = expose('flash exptime=20 cams=b1 doIIS', prepare=dict(iis=dict(hgar=20)),
                 inject=flashEndOnOpen('abort'))
    assert res.cmd.didFail, 'aborted flash reported success'
    assert res.sent('enu_sm1', 'exposure finish'), 'the shutters were never told to close'
    assert res.stopped('iis') == 1, 'iis left burning after the flash was aborted'


def test_ending_no_flash_is_refused():
    res = expose('flash abort')
    assert res.cmd.didFail, 'abort with no flash ongoing was accepted'


def test_abort_during_warmup_releases_the_warming_lamp():
    """The warm-up already lit it, though it never had its go."""

    def inject(sim, cmdSet):
        sim.onCommand('iis', 'waitForReadySignal', lambda: cmdSet.call('flash abort'))

    res = expose(f'flash exptime={EXPTIME} cams=b1 doIIS', prepare=dict(iis=dict(hgar=EXPTIME)), iisWarmup=1,
                 inject=inject)
    assert res.cmd.didFail, 'aborted flash reported success'
    assert not res.sent('enu_sm1', 'shutters expose'), 'shutters opened for an aborted flash'
    assert res.stopped('iis') == 1, f'warming iis released {res.stopped("iis")} times, expected once'


def test_go_failure_fails_the_flash_and_releases_the_lamp():
    res = expose(f'flash exptime={EXPTIME} cams=b1 doIIS', prepare=dict(iis=dict(hgar=EXPTIME)),
                 inject=failAt('iis', 'go'))
    assert res.cmd.didFail, 'a flash with no light reported success'
    assert res.sent('enu_sm1', 'exposure finish'), 'the shutters were never told to close'
    assert res.stopped('iis') == 1, 'iis not released after a failed flash'


def test_shutter_failure_before_opening_never_fires_the_lamps():
    res = expose(f'flash exptime={EXPTIME} cams=b1 doLamps isLast', prepare=dict(pfilamps=dict(neon=EXPTIME)),
                 inject=failAt('enu_sm1', 'shutters expose, before opening'))
    assert res.cmd.didFail, 'the shutter failure was not reported'
    assert not res.sent('pfilamps', 'go'), 'lamps fired with the shutters closed'
