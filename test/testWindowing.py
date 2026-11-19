"""Scenarios on windowed and row-binned exposures: how the ccds are wiped and read.

A windowed exposure reads `row0=R0 nrows=NR`, a binned one a full frame with `rowbin=N`;
both wipe without flushing, `wipe nrows=0`, and leave the n cameras out.
"""

from testExposure import expose

READ_ROWS = 4300


def test_windowed_exposure_reads_its_window():
    res = expose('expose object exptime=0.4 cams=b1,r1 visit=1 window=500,1000')
    assert res.fileIds and 'b1' in res.fileIds and 'r1' in res.fileIds, res.fileIds

    for ccd in ('ccd_b1', 'ccd_r1'):
        assert res.sent(ccd, 'wipe') == ['wipe nrows=0'], res.sent(ccd, 'wipe')
        [read] = res.sent(ccd, 'read')
        assert 'row0=500 nrows=1000' in read and 'rowbin' not in read, read


def test_binned_exposure_reads_the_full_frame_binned():
    res = expose('expose object exptime=0.4 cams=b1,r1 visit=1 rowbin=25')
    assert res.fileIds and 'b1' in res.fileIds and 'r1' in res.fileIds, res.fileIds

    for ccd in ('ccd_b1', 'ccd_r1'):
        assert res.sent(ccd, 'wipe') == ['wipe nrows=0'], res.sent(ccd, 'wipe')
        [read] = res.sent(ccd, 'read')
        assert 'rowbin=25' in read and 'row0' not in read and 'nrows' not in read, read


def test_unbinned_exposure_wipes_and_reads_the_full_frame():
    # Negative control: without a window or rowbin, a full wipe and a plain read.
    res = expose('expose object exptime=0.4 cams=b1 visit=1')
    assert res.sent('ccd_b1', 'wipe') == ['wipe'], res.sent('ccd_b1', 'wipe')
    [read] = res.sent('ccd_b1', 'read')
    assert 'rowbin' not in read and 'row0' not in read, read


def test_binned_exposure_takes_no_h4():
    res = expose('expose object exptime=0.4 cams=b1,n1 visit=1 rowbin=25')
    assert not res.sent('hx_n1'), 'a binned exposure started an H4 ramp'
    assert 'ignoring nir cameras for binned exposure.' in ' '.join(res.cmd.says('w')), res.cmd.says('w')
    assert res.fileIds and 'b1' in res.fileIds, res.fileIds


def test_rowbin_has_to_divide_the_ccd_rows():
    for rowbin in (16, 0, -5):
        res = expose(f'expose object exptime=0.4 cams=b1 visit=1 rowbin={rowbin}')
        assert res.cmd.didFail, f'rowbin={rowbin} was accepted'
        assert 'does not evenly divide' in ' '.join(res.cmd.says('f')), res.cmd.says('f')
        assert not res.sent('ccd_b1'), f'rowbin={rowbin}: the ccd was exposed'

    # Negative control: every divisor of the ccd rows up to 100 is accepted.
    for rowbin in (rowbin for rowbin in range(1, 101) if not READ_ROWS % rowbin):
        assert not expose(f'expose bias cams=b1 visit=1 rowbin={rowbin}').cmd.didFail, rowbin


def test_rowbin_and_window_cannot_be_combined():
    for window in ('window=500,1000', 'blueWindow=500,1000', 'redWindow=500,1000'):
        res = expose(f'expose object exptime=0.4 cams=b1,r1 visit=1 rowbin=25 {window}')
        assert res.cmd.didFail, f'rowbin with {window} was accepted'
        assert 'cannot be combined' in ' '.join(res.cmd.says('f')), res.cmd.says('f')
        assert not res.sent('ccd_b1') and not res.sent('ccd_r1'), 'a ccd was exposed'
