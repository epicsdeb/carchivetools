# -*- coding: utf-8 -*-

from __future__ import print_function
import os
from twisted.internet import defer
from carchive.date import makeTimeInterval
from carchive.pb import granularity, filepath, exporter

class PbExportError(Exception):
    pass

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)
    
    # Get out dir.
    if opt.export_out_dir is None:
        raise PbExportError('Output directory not specified!')
    out_dir = opt.export_out_dir
    
    # Get granularity.
    if opt.export_granularity is None:
        raise PbExportError('Export granularity not specified!')
    gran = granularity.get_granularity(opt.export_granularity)
    if gran is None:
        raise PbExportError('Export granularity is not understood!')
    
    # Collect PV name delimiters.
    delimiters = ([] if opt.export_no_default_delimiters else [':', '-']) + \
        ([] if opt.export_delimiter is None else opt.export_delimiter)
    
    # Collect PV name patterns.
    patterns = []
    if opt.export_all:
        patterns.append('.*')
    if opt.export_pattern is not None:
        patterns += opt.export_pattern
    
    # Collect PVs to archive...
    pvs = set()
    
    # Query PVs for patterns.
    for pattern in patterns:
        print('Querying pattern: {}'.format(pattern))
        search_result = yield archive.search(pattern=pattern, archs=archs)
        print('--> {} PVs.'.format(len(search_result)))
        pvs.update(search_result)

    # Add explicit PVs.
    pvs.update(args)
    
    # Sort PVs.
    pvs = sorted(pvs)
    
    # Check we have any PVs.
    if len(pvs)==0:
        raise PbExportError('Have no PV names to archive!')
    
    # Resolve time interval.
    T0, Tend = makeTimeInterval(opt.start, opt.end)
    
    # Print some info.
    print('-- Requested time range: {} -> {}'.format(T0, Tend))
    print('-- Will archive these PVs: {}'.format(' '.join(pvs)))
    
    # Remembering PVs which we had problems with.
    failed_pvs = []
    
    # Archive PVs one by one.
    for pv in pvs:
        print('-- Archiving PV: {}'.format(pv))
        
        # Create exporter instance.
        with exporter.Exporter(pv, gran, out_dir, delimiters) as the_exporter:
            # TBD bound range by last sample
            pv_start_t = T0
            pv_end_t = Tend
            
            try:
                # Ask for samples.
                segment_data = yield archive.fetchraw(
                    pv, the_exporter, archs=archs, cbArgs=(),
                    T0=pv_start_t, Tend=pv_end_t, chunkSize=opt.chunk,
                    enumAsInt=True, provideExtraMeta=True
                )
            except exporter.SkipPvError as e:
                print('-- PV NOT SUCCESSFUL: {}: {}'.format(pv, e))
                failed_pvs.append((pv, e))
                break
    
    print('-- ALL DONE')
    
    if len(failed_pvs) > 0:
        print('ERROR summary:')
        for (pv, e) in failed_pvs:
            print('{}: {}'.format(pv, e))
    
    defer.returnValue(0)
