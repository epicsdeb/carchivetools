from __future__ import print_function

import datetime, re
from carchive.backend.pb.filepath import make_sure_path_exists

'''
    Dumps the mysql insert statement into the files named disconnected_<timestamp_now>.sql
    and connected_<timestamp_now>.sql. One contains connected PVs and the other one disconnected.
    The connection state is determined based on the archived data and not on the actual PV connection
    state. The PV is considered disconnected if it is disconnected, or archiving was stopped.
    
    Note that the statement is using a predefined template, which may not be complete and identical 
    to what the appliance would store if the PV were added using the appliance web application.
    The template is filled with all possible data that can be obtained from the original source
    (limits, units, precision, name), but the data that the appliance defines at runtime
    (storage rate, sampling period, host name, data store etc.) are kept at default values.
'''

chunk = re.compile(r'[:-]')

template = ('(\'{0}\',\'{{"upperDisplayLimit":"{1}","lowerDisplayLimit":"{2}",'
            '"upperAlarmLimit":"{3}","lowerAlarmLimit":"{4}",'
            '"upperWarningLimit":"{5}","lowerWarningLimit":"{6}",'
            '"upperCtrlLimit":"{7}","lowerCtrlLimit":"{8}",'
            '"precision":"{9}","units":"{10}",'
            '"scalar":"{11}","elementCount":"{12}",'
            '"pvName":"{13}",'
            '"DBRType":"{14}",'
            '"samplingMethod":"MONITOR",'
            '"computedStorageRate":"0.0","computedBytesPerEvent":"0","computedEventRate":"0.0",'
            '"userSpecifiedEventRate":"0.0","samplingPeriod":"0.0",'
            '"extraFields":{{"NAME":"{15}","RTYP":"","SCAN":"0.0"}},'
            '"hostName":"0.0.0.0",'
            '"hasReducedDataSet":"false","chunkKey":"{16}:",'
            '"applianceIdentity":"{17}",'
            '"paused":"false","archiveFields":[{18}],'
            '"creationTime":"{19}","modificationTime":"{20}",'
            '"dataStores":['
            '"pb:\/\/localhost?name=STS&rootFolder=${{ARCHAPPL_SHORT_TERM_FOLDER}}'
            '&partitionGranularity=PARTITION_HOUR&consolidateOnShutdown=true",'
            '"pb:\/\/localhost?name=MTS&rootFolder=${{ARCHAPPL_MEDIUM_TERM_FOLDER}}'
            '&partitionGranularity=PARTITION_DAY&hold=2&gather=1",'
            '"pb:\/\/localhost?name=LTS&rootFolder=${{ARCHAPPL_LONG_TERM_FOLDER}}'
            '&partitionGranularity=PARTITION_YEAR"]}}\',\'{21}\')')

class _MyInfo(object):
    def __init__(self, name, hdisp, ldisp, halarm, lalarm, hwarn, lwarn,
                     hctrl, lctrl, prec, units, scalar, ncount, pv_type):
        self._name = name
        self._hdisp = hdisp
        self._ldisp = ldisp
        self._halarm = halarm
        self._lalarm = lalarm
        self._hwarn = hwarn
        self._lwarn = lwarn
        self._lctrl = lctrl
        self._hctrl = hctrl
        self._prec = prec
        self._units = units
        self._scalar = 'true' if scalar else 'false'
        self._ncount = ncount
        self._pv_type = pv_type
        self._fields = ''
        self._pv_disconnected = False
        if scalar:
            self._fields = '"LOLO","HIGH","LOPR","LOW","HOPR","HIHI"'        

class MySqlWriter(object):
    def __init__(self, out_dir, appl, write_connected=False):
        self._appl = appl
        self._write_connected = write_connected
        
        now = datetime.datetime.now()
        self._time = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'
        self._time_field = now.strftime('%Y-%m-%d %H:%M:%S')
        
        make_sure_path_exists(out_dir)
        suffix = now.strftime('%Y-%m-%dT%H%M%S%f')[:-3]
        self._dis_file = open(out_dir + '/disconnected_'+ suffix +'.sql'  , 'a')
        self._dis_file.write('insert into PVTypeInfo VALUES ')
        
        self._con_file = None
        if self._write_connected:
            self._con_file = open(out_dir + '/connected_'+ suffix +'.sql'  , 'a')
            self._con_file.write('insert into PVTypeInfo VALUES ')
        
        self._dis_first_info_written = False
        self._con_first_info_written = False
        self._last_pv_info = None
    
    def close(self):
        ''' Close the output stream. '''
        if self._dis_file is not None:
            self._dis_file.write(';\n')
            self._dis_file.close()
        if self._con_file is not None:
            self._con_file.write(';\n')
            self._con_file.close()
            
    def put_pv_info(self, name, hdisp=0.0, ldisp=0.0, halarm=0.0, lalarm=0.0, hwarn=0.0, lwarn=0.0,
                      hctrl=0.0, lctrl=0.0, prec=1.0, units='',scalar=True,ncount=1,pv_type=''):
        ''' Store the information for the pv identified by the name. These data can be written
        to the file using the #write_pv_info routine. '''
        self._last_pv_info = _MyInfo(name, hdisp, ldisp, halarm, lalarm, hwarn, lwarn, hctrl, lctrl,
                                     prec, units, scalar, ncount, pv_type)
    
    def pv_disconnected(self, pv_name):
        ''' Mark the current pv if it matches the pv_name as disconnected. '''
        if self._last_pv_info._name == pv_name:
            self._last_pv_info._pv_disconnected = True
    
    def write_pv_info(self):
        ''' Write the pv info to file. If write_connected is true the info will be written
        regardless of the current pv state, if False the info will be written only if
        the pv is disconnected.'''
        if self._last_pv_info is None:
            return
        
        info = self._last_pv_info;
        nn = chunk.sub('\/',info._name)
        val = template.format(info._name,info._hdisp,info._ldisp,info._halarm,info._lalarm,info._hwarn,
                              info._lwarn,info._hctrl,info._lctrl,info._prec,info._units,info._scalar,
                              info._ncount,info._name,info._pv_type,info._name,nn,
                              self._appl,info._fields,self._time,self._time,self._time_field)
        
        if self._last_pv_info._pv_disconnected: 
            if self._dis_first_info_written:
                self._dis_file.write(',\n');
            self._dis_first_info_written=True
            self._dis_file.write(val)
            self._dis_file.flush()
        elif self._write_connected:
            if self._con_first_info_written:
                self._con_file.write(',\n');
            self._con_first_info_written=True
            self._con_file.write(val)
            self._con_file.flush()
        self._last_pv_info = None
