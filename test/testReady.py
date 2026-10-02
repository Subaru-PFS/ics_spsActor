"""Scenarios for `sps checkReady`, the checks an exposure needs taken on their own.

checkReady is sent before an exposure's lamps are warmed, so whatever stops the cameras
from exposing is reported before a lamp is spent on them. Each scenario sets the state an
enu reports, then checks what checkReady concludes; the last two check that `expose` itself
checks nothing, while still accepting the flags older clients send it.
"""

import ics.utils.time as pfsTime
from spsActor.Commands.ExposeCmd import ExposeCmd

from simulator.actor import Sim
from simulator.command import CmdSet

from testExposure import expose


def checkReady(cmdStr, specNums=(1,), state=None, timeout=10):
    """Send one command to a fresh simulated spectrograph whose enus report `state`.

    state maps an enu actor name to the keywords it should report, e.g.
    dict(enu_sm1=dict(bia='on')).
    """
    sim = Sim(specNums=specNums)
    cmdSet = CmdSet(sim, ExposeCmd)

    for enuName, keys in (state or dict()).items():
        for key, value in keys.items():
            sim.declareModel(enuName).keyVarDict[key].set(value)

    cmd = cmdSet.call(cmdStr)
    deadline = pfsTime.timestamp() + timeout

    while not cmd.isDone:
        if pfsTime.timestamp() > deadline:
            raise TimeoutError(f'{cmdStr!r} never concluded')

        pfsTime.sleep.millisec()

    return cmd


def failure(cmd):
    """The text a failed command concluded with."""
    return ' '.join(cmd.says('f'))


def test_ready_when_slit_home_and_bia_off():
    cmd = checkReady('checkReady cams=b1 doScienceCheck')
    assert not cmd.didFail, failure(cmd)


def test_slit_out_of_home_is_not_ready():
    cmd = checkReady('checkReady cams=b1 doScienceCheck', state=dict(enu_sm1=dict(slitPosition='undef')))
    assert cmd.didFail, 'ready with the slit out of home'
    assert 'SlitPositionError(sm1=undef)' in failure(cmd), failure(cmd)


def test_bia_on_is_not_ready():
    cmd = checkReady('checkReady cams=b1', state=dict(enu_sm1=dict(bia='on')))
    assert cmd.didFail, 'ready with the bia on'
    assert 'BIA is ON for spectrographs sm1' in failure(cmd), failure(cmd)


def test_slit_and_bia_are_reported_together():
    cmd = checkReady('checkReady cams=b1 doScienceCheck',
                     state=dict(enu_sm1=dict(slitPosition='undef', bia='on')))
    assert cmd.didFail
    assert 'SlitPositionError' in failure(cmd) and 'BIA is ON' in failure(cmd), failure(cmd)


def test_every_spectrograph_is_checked():
    cmd = checkReady('checkReady cams=b1,b2', specNums=(1, 2), state=dict(enu_sm2=dict(bia='on')))
    assert cmd.didFail
    assert 'spectrographs sm2' in failure(cmd), failure(cmd)


def test_skip_bia_check_ignores_the_bia():
    cmd = checkReady('checkReady cams=b1 skipBiaCheck', state=dict(enu_sm1=dict(bia='on')))
    assert not cmd.didFail, failure(cmd)


def test_slit_is_not_checked_without_science_check():
    cmd = checkReady('checkReady cams=b1', state=dict(enu_sm1=dict(slitPosition='undef')))
    assert not cmd.didFail, failure(cmd)


def test_expose_checks_nothing_itself():
    def notReady(sim, cmdSet):
        sim.declareModel('enu_sm1').keyVarDict['slitPosition'].set('undef')
        sim.declareModel('enu_sm1').keyVarDict['bia'].set('on')

    res = expose('expose object exptime=0.4 cams=b1 visit=1 doScienceCheck isLast', inject=notReady)
    assert not res.cmd.didFail, ' '.join(res.cmd.says('f'))
    assert res.fileIds, 'no file produced'
    assert res.sent('enu_sm1', 'shutters'), 'never opened the shutters'


def test_expose_still_accepts_skip_bia_check():
    res = expose('expose object exptime=0.4 cams=b1 visit=1 skipBiaCheck isLast')
    assert not res.cmd.didFail, ' '.join(res.cmd.says('f'))
    assert res.fileIds, 'no file produced'
