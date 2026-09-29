"""Scenarios on how an H4 ramp is sized and where it ends.

Times run at 1:20: an IRP4 read is 0.35 s instead of 6.924 s, an IRP1 read 0.543 s instead
of 10.857 s, a full-frame ccd readout 2 s instead of 40 s.  An H4 read ends at the times
the simulated device records; it started one read time earlier.
"""
from testExposure import expose

IRP4, IRP1 = dict(h4ReadTime=0.35, irpRatio=4), dict(h4ReadTime=0.543, irpRatio=1)
CCD_READ = 2.0


def plannedReads(res, cam):
    [ramp] = [cmdStr for cmdStr in res.sent(f'hx_{cam}', 'ramp nread')]
    return int(ramp.split('nread=')[1].split()[0])


def readsFullyAfter(hx, when):
    """How many reads of the ramp started at or after `when`."""
    return sum(1 for end in hx.readsAt if end - hx.readTime >= when)


def firstRowsAt(ccd, fraction):
    """When the ccd first reported having read out at least `fraction` of its rows."""
    return min(t for t, done in ccd.rowsAt if done >= fraction)


def test_nir_only_ramp_takes_its_extra_reads_by_irp():
    res4 = expose('expose object exptime=1.0 cams=n1 visit=1 isLast', **IRP4)
    assert plannedReads(res4, 'n1') == int(1.0 // 0.35) + 3 + 3, plannedReads(res4, 'n1')

    res1 = expose('expose object exptime=1.0 cams=n1 visit=1 isLast', **IRP1)
    assert plannedReads(res1, 'n1') == int(1.0 // 0.543) + 3 + 1, plannedReads(res1, 'n1')


def test_nir_only_ramp_is_finished_at_shutter_close():
    res = expose('expose object exptime=1.0 cams=n1 visit=1 isLast', **IRP4)
    hx, closedAt = res.sim.devices['hx_n1'], res.sim.devices['enu_sm1'].closedAt
    assert hx.finishes, 'the ramp was never told to finish'
    assert closedAt <= hx.finishes[0][0] <= closedAt + 2 * hx.readTime, 'finished away from the shutter close'


def test_mixed_ramp_reads_through_the_ccd_readout():
    res = expose('expose object exptime=1.0 cams=b1,n1 visit=1 isLast', ccdReadTime=CCD_READ, **IRP4)
    hx, ccd = res.sim.devices['hx_n1'], res.sim.devices['ccd_b1']
    assert res.fileIds and 'n1' in res.fileIds and 'b1' in res.fileIds, res.fileIds

    assert hx.finishes, 'the ramp was never told to finish'
    assert hx.finishes[0][0] >= firstRowsAt(ccd, 0.8), 'told to finish before the ccds reached 80%'

    ccdDone = ccd.rowsAt[-1][0]
    assert abs(hx.readsAt[-1] - ccdDone) <= 2 * hx.readTime, \
        f'ramp ended {hx.readsAt[-1] - ccdDone:+.2f}s from the end of the ccd readout'
    assert len(hx.readsAt) < plannedReads(res, 'n1'), 'the ramp ran to its ceiling'


def test_mixed_ramp_threshold_follows_irp():
    """The finish goes out at the first read after the threshold, so the readout is made long
    enough for 55% and 80% to be further apart than a read."""
    res = expose('expose object exptime=1.0 cams=b1,n1 visit=1 isLast', ccdReadTime=6.0, **IRP1)
    hx, ccd = res.sim.devices['hx_n1'], res.sim.devices['ccd_b1']
    assert hx.finishes, 'the ramp was never told to finish'

    finishedAt, at55, at80 = hx.finishes[0][0], firstRowsAt(ccd, 0.55), firstRowsAt(ccd, 0.8)
    assert at55 <= finishedAt <= at55 + hx.readTime + 0.1, f'finished {finishedAt - at55:+.2f}s from the 55% message'
    assert finishedAt < at80, 'IRP1 waited for 80%'


def test_iis_flat_leaves_an_unlit_difference_after_the_lamp():
    """INSTRM-3024: n1 reaches its first read 1.2 reads before n2 and waits for it, and the
    iis lamp lights 4 s (0.2 s here) after its go; one full difference must still follow
    the lamp going out, on every camera."""
    res = expose('expose flat exptime=1.5 cams=n1,n2 visit=1 doIIS isLast', specNums=(1, 2),
                 prepare=dict(iis=dict(halogen=1.5)), hxStartup={1: 0.05, 2: 0.05 + 1.2 * 0.35},
                 iisGoLatency=0.2, **IRP4)
    offAt = res.sim.lamps('iis').offAt
    assert offAt, 'the iis lamp never went out'

    for cam in ('n1', 'n2'):
        after = readsFullyAfter(res.sim.devices[f'hx_{cam}'], offAt)
        assert after >= 2, f'{cam}: {after} read(s) after the lamp went out, an unlit difference needs 2'


def test_ccd_read_failure_still_ends_the_mixed_ramp():
    res = expose('expose object exptime=1.0 cams=b1,n1 visit=1 isLast', ccdReadTime=CCD_READ,
                 inject=lambda sim, cmdSet: sim.failCommand('ccd_b1', 'read'), **IRP4)
    hx = res.sim.devices['hx_n1']
    assert hx.finishes, 'no ccd will ever reach the threshold, yet the ramp was never told to finish'
    assert len(hx.readsAt) < plannedReads(res, 'n1'), 'the ramp ran to its ceiling'


def test_dark_ramp_is_sized_to_the_dark_and_left_alone():
    res = expose('expose dark exptime=1.0 cams=b1,n1 visit=1', ccdReadTime=CCD_READ, **IRP4)
    assert plannedReads(res, 'n1') == round(1.0 / 0.35) + 1, plannedReads(res, 'n1')
    assert not res.sim.devices['hx_n1'].finishes, 'a dark ramp was told to finish'


def test_windowed_exposure_takes_no_h4():
    res = expose('expose object exptime=1.0 cams=b1,n1 visit=1 window=500,1000', ccdReadTime=CCD_READ, **IRP4)
    assert not res.sent('hx_n1'), 'a windowed exposure started an H4 ramp'
