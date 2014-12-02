carchivetools
=============

This package provides command line tools
to retrieve data from [EPICS][] Channel Archiver
and Archive Appliance data storage services.

The commands argrep and arget are provided.
See the [man page](arget.pod) for details.

In addition to the user tools, two backend servers
[a2aproxy](README-a2aproxy.md) and [archmiddle](README-archmiddle.md)
are provided.

[EPICS]: http://www.aps.anl.gov/epics/

Requirements
------------

Basic requirements.

* [Python](http://www.python.org/) >=2.6, &lt;3.0 (&gt;= 2.7 required for Appliance)
* [Twisted](http://twistedmatrix.com/) Core and Web >= 10.1
* [Numpy](http://www.numpy.org/) >= 1.4

Additional requirements for Archive Appliance support.

* Google [Protocol Buffers](http://code.google.com/p/protobuf/) C++ library and Python module

Configuration
-------------

In addition to command line arguements, configuration
may be given in the following locations.
See the [example configuration](carchive.conf.example) for details.

* /etc/carchive.conf
* $HOME/.carchiverc
* $PWD/carchiver.conf

Development Builds
------------------

Those wishing to make modifications to this code may find it easier to
do an in-place build of the pbdecode Extension (as opposed to "setup.py install").

    $ python setup.py build_protobuf -i
    $ python setup.py build_ext -i

