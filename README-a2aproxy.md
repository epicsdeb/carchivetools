Archiver to Appliance Proxy
===========================

The a2aproxy allows existing Channel Archiver clients
to query an Archiver Appliance without modification.

It acts as an XMLRPC server implementing the Archiver dataserver
calls.  These calls are translated into appropriate Appliance
requests.

The only configuration which is necessary is to provide the TCP
port on which the XMLRPC server will run, and the hostname/URL
of the Appliance server.

For example,

    $ twisted -n a2aproxy -P 8888 -A capp01.cs.nsls2.local:17665

On a server named 'myproxy.cs.nsls2.local' will allow Archiver clients
to be configured with

    http://myproxy.cs.nsls2.local:8888/cgi-bin/ArchiveDataServer.cgi

Equivalently, the full URL of the getApplianceInfo URL can be provided

    $ twisted -n a2aproxy -P 8888 -A http://capp01.cs.nsls2.local:17665/mgmt/bpl/getApplianceInfo

Details
-------

The a2aproxy will report only one archiver key #42 named "All".

However, it will ignore the key number in requests.
This is intended to easy migration of existing client
configurations, but may cause problems for clients
which can't handle keys with overlapping time ranges.

