carchivetools
=============

This package provides command line tools
to retrieve data from [EPICS][] Channel Archiver
and Archive Appliance data storage services.

The commands argrep and arget are provided.
See the [man page](arget.pod) for details.

[EPICS]: http://www.aps.anl.gov/epics/

Requirements
------------

Basic requirements.

* [Python](http://www.python.org/) 2.6 or 2.7
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
