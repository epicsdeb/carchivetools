# -*- coding: utf-8 -*-
"""
This software is Copyright by the
 Board of Trustees of Michigan
 State University (c) Copyright 2015.
"""
from __future__ import print_function
import datetime
from twisted.internet import defer
from carchive.backend.pb import granularity as pb_granularity
from carchive.backend.pb import exporter as pb_exporter
from carchive.backend.pb import last as pb_last
from carchive.backend.pb import timestamp as pb_timestamp
from carchive.backend.pb import pvlog as pb_pvlog
from carchive.backend.pb import mysql as pb_mysql
import logging
from logging import INFO

_log = logging.getLogger('carchive.pbrawexport')
_log.setLevel(INFO)

class PbExportError(Exception):
    pass

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    archs = opt.archive
    
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
    
    appliance_name = 'appliance0'
    if opt.appliance_name is not None:
        appliance_name = opt.appliance_name
    
    mysql_write_connected = False
    if opt.mysql_write_connected is not None:
        mysql_write_connected = True
        
    
    # Collect PV name delimiters.
    delimiters = ([] if opt.export_no_default_delimiters else [':', '-']) + \
        ([] if opt.export_delimiter is None else opt.export_delimiter)
        
    # Collect PVs to archive...
    pvs = set()
    # Add explicit PVs.
    pvs.update(args)
    # Sort PVs.
    pvs = sorted(pvs)
    
    # Check we have any PVs.
    if len(pvs)==0:
        raise PbExportError('No PVs found to export data!')
            
    # Parse start/end times. These give us the native format for the query.
    
    start_ca_t = parse_time(opt.start, 'start')
    end_ca_t = parse_time(opt.end, 'end')
    
    # Print some info.
    _log.info('Will export data of these PVs: {0}'.format(', '.join(pvs)))
    
    # Keep PV-specific logs.
    pv_logs = []
    
    mysql_writer = pb_mysql.MySqlWriter(out_dir,appliance_name,delimiters,mysql_write_connected)
    
    # Archive PVs one by one.
    for pv in pvs:
        _log.info('Exporting data for PV: {0}'.format(pv))
        
        # Create and remember a PvLog object.
        pvlog = pb_pvlog.PvLog(pv)
        pv_logs.append(pvlog)
        
        # Find the last sample timestamp for this PV.
        # This is used as-is as a lower bound filter after the query.
        last_timestamp = pb_last.find_last_sample_timestamp(pv, out_dir, gran, delimiters)
        
        pvlog.info('Last timestamp: {0}'.format(last_timestamp))
        
        # We don't want samples <=last_timestamp, we can't write those out.
        # Due to conversion errors, we limit the query conservatively, and filter out any
        # initial samples we get that we don't want.
        if last_timestamp is not None:
            low_limit_dt = pb_timestamp.pb_to_dt(*last_timestamp) - datetime.timedelta(seconds=1)
            query_start_ca_t = max(start_ca_t, pb_timestamp.dt_to_carchive(low_limit_dt))
        else:
            query_start_ca_t = start_ca_t
        
        pvlog.info('Query low limit: {0}'.format(query_start_ca_t))
        # Create exporter instance.
        with pb_exporter.Exporter(pv, gran, out_dir, delimiters, last_timestamp, pvlog, mysql_writer) as the_exporter:
            try:
                # Ask for samples.
                segment_data = yield archive.fetchraw(
                    pv, the_exporter, archs=archs, cbArgs=(),
                    T0=query_start_ca_t, Tend=end_ca_t, chunkSize=opt.chunk,
                    enumAsInt=True, displayMeta=True, rawTimes=True
                )
            except pb_exporter.SkipPvError as e:
                _log.error('PV ERROR: {0}: {1}'.format(pv, e))
                pvlog.error(str(e))
                break
        #In case the pv is disconnected, write the last sample and include the cnxlostepsecs    
        the_exporter.write_last_disconnected()
        #Write the pv info to mysql
        mysql_writer.write_pv_info()
        
    mysql_writer.close()
    _log.info('ALL DONE, REPORT FOLLOWS\n')
    
    # Print out logs.
    for pvlog in pv_logs:
        report = pvlog.build_report()
        _log.info(report)
    
    defer.returnValue(0)

TIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
]

def parse_time(time_str, role):
    # If there is no argument, assume unbounded.
    # But we do need to use something in the query.
    if time_str is None:
        return {'start': (-2**31, 0), 'end':(2**31-1, 999999999)}[role]
    
    # Try parsing the time string as different supported formats.
    for fmt in TIME_FORMATS:
        try:
            dt = datetime.datetime.strptime(time_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError('The {0} time argument is not understood. Supported formats are: {1}'.format(role, TIME_FORMATS))
    
    # Convert to the format for the query.
    return pb_timestamp.dt_to_carchive(dt)
