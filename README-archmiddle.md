Archive Middle Layer Proxy
==========================

The archmiddle proxy attempts to simplify configuration of
Channel Archiver clients by providing one virtual archive key
which is backed by a number of keys for non-overlaping sets of PVs.

Archmiddle is [configured](archmiddle.conf) with a one to many mapping
of key names.
For example, with a dataserver providing keys 'Vac/Current' and 'RF/Current',
archmiddle could be configured with

    [mapping]
    All/Current = 1 Vac/Current RF/Current

This defines virtual key #1 named 'All/Current' which will act against
the two keys listed.

Alternately

    [mapping]
    All/Current = 1 */Current

Defines virtual key #1 to act against all keys with names ending in '/Current'.

The goal is to simplify the situation where many ArchiveEngines are
writing index files.
Generating an overall index file introduces extra latency and load.

Details
-------

The archmiddle proxy treats the four dataserver RPC calls as follows.

_archiver.info_

This call is passed through to the dataserver without modifications.

_archiver.archives_

This call is answered by the archmiddle proxy with a list of the
virtual keys configured.

_archiver.names_

This call triggers a corresponding names() call to the dataserver
for each of the keys underlaying the virtual key.
The results of these queries are merged and a single reply is
returned to the caller.

Replies are combined by PV name.
If more than one query yields a result for the same PV name,
then all but one of these results will be _ignored_.

_archiver.values_

Is passed though with the virtual key number replaced with
the actual key number where this PV is found.

The values call is not fulling implemented.
Making a single call for more than one PV name will fail
unless all of the requested names are provided by a single dataserver key.

