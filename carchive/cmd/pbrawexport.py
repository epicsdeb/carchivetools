# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import datetime
from twisted.internet import defer
from carchive.date import makeTimeInterval
from carchive.pb import granularity as pb_granularity
from carchive.pb import exporter as pb_exporter
from carchive.pb import last as pb_last
from carchive.pb import timestamp as pb_timestamp

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
    gran = pb_granularity.get_granularity(opt.export_granularity)
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
    start_dt, end_dt = makeTimeInterval(opt.start, opt.end)
    
    # Convert the interval to old archiver time format.
    # There is some error here.
    start_ca_t = pb_timestamp.dt_to_carchive(start_dt)
    end_ca_t = pb_timestamp.dt_to_carchive(end_dt)
    
    # Print some info.
    print('-- Requested time range: {} -> {}'.format(start_dt, end_dt))
    print('-- Requested time range after conversion: {} -> {}'.format(start_ca_t, end_ca_t))
    print('-- Will archive these PVs: {}'.format(' '.join(pvs)))
    
    # Remembering PVs which we had problems with.
    failed_pvs = []
    
    # Archive PVs one by one.
    for pv in pvs:
        print('-- Archiving PV: {}'.format(pv))
        
        # Find the last sample timestamp for this PV.
        # This is used as-is as a lower bound filter after the query.
        last_timestamp = pb_last.find_last_sample_timestamp(pv, out_dir, gran, delimiters)
        
        print('Last timestamp: {}'.format(last_timestamp))
        
        # We don't want samples <=last_timestamp, we can't write those out.
        # Due to conversion errors, we limit the query conservatively, and filter out any
        # initial samples we get that we don't want.
        if last_timestamp is not None:
            low_limit_dt = pb_timestamp.pb_to_dt(*last_timestamp) - datetime.timedelta(seconds=1)
            query_start_ca_t = max(start_ca_t, pb_timestamp.dt_to_carchive(low_limit_dt))
        else:
            query_start_ca_t = start_ca_t
        
        print('Query low limit: {}'.format(query_start_ca_t))
        
        # Create exporter instance.
        with pb_exporter.Exporter(pv, gran, out_dir, delimiters, last_timestamp) as the_exporter:
            try:
                # Ask for samples.
                segment_data = yield archive.fetchraw(
                    pv, the_exporter, archs=archs, cbArgs=(),
                    T0=query_start_ca_t, Tend=end_ca_t, chunkSize=opt.chunk,
                    enumAsInt=True, provideExtraMeta=True, rawTimes=True
                )
            except pb_exporter.SkipPvError as e:
                print('-- PV ERROR: {}: {}'.format(pv, e))
                failed_pvs.append((pv, e))
                break
    
    print('-- ALL DONE')
    
    if len(failed_pvs) > 0:
        print('ERROR summary:')
        for (pv, e) in failed_pvs:
            print('{}: {}'.format(pv, e))
    
    defer.returnValue(0)
