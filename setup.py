#!/usr/bin/env python
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

import subprocess as sp

from distutils.core import setup, Distribution, Extension, Command, DistutilsSetupError
from distutils.command import build, build_ext, install

from numpy.distutils.misc_util import get_numpy_include_dirs

class GenProtobuf(Command):
    """Run protoc code generator
    """
    user_options = [
        ('protoc=', 'P', "protobuf compiler"),
        ('build-lib=', 'b',
         "directory for compiled extension modules"),
        ('build-temp=', 't',
         "directory for temporary files (build by-products)"),
        ('inplace', 'i',
         "ignore build-lib and put compiled extensions into the source " +
         "directory alongside your pure Python modules"),
    ]

    def initialize_options(self):
        self.protoc = 'protoc'
        self.proto = None
        self.build_temp = None
        self.build_lib = None
        self.inplace = 0

    def finalize_options(self):
        self.proto = self.distribution.x_proto or ()

        self.set_undefined_options('build',
                                   ('build_temp', 'build_temp'),
                                   ('build_lib', 'build_lib'),
                                  )
        self.set_undefined_options('build_ext',
                                   ('inplace','inplace'),
                                  )

        from os.path import isfile
        if self.protoc and not isfile(self.protoc):
            from distutils.spawn import find_executable
            self.protoc = find_executable(self.protoc)
        if not self.protoc:
            raise DistutilsSetupError("Unable to find 'protoc'")

    def run(self):
        outtemp, outlib = self.build_temp, self.build_lib
        if self.inplace:
            outtemp, outlib = '.', '.'
        else:
            self.mkpath(outtemp)
            self.mkpath(outlib)

        for pbd, opts in self.proto:
            if opts.get('py', False):
                self.spawn([self.protoc, '--python_out='+outlib, pbd])
            if opts.get('cpp', False):
                self.spawn([self.protoc, '--cpp_out='+outtemp, pbd])

class BuildExtGen(build_ext.build_ext):
    """Extend -I search path to find generated files
    """
    def finalize_options(self):
        build_ext.build_ext.finalize_options(self)
        # Search for generated headers referenced from top level (eg. carchive/backend/...)
        if self.inplace:
            self.include_dirs.append('.')
        else:
            self.include_dirs.append(self.build_temp)

class LinkScripts(Command):
    """Symlink or copy scripts
    """
    def initialize_options(self):
        self.install_dir = None
    def finalize_options(self):
        self.set_undefined_options('install',
                                   ('install_scripts', 'install_dir'))
        self.links = self.distribution.x_link_scripts or ()
    def run(self):
        from os.path import join
        for target, dest in self.links:
            self.copy_file(target,
                           join(self.install_dir, dest),
                           link='sym')

# Allow setup(x_proto=...)
Distribution.x_proto = None
Distribution.x_link_scripts = None

# Hook into build command early.
build.build.sub_commands.insert(0, ('build_protobuf', lambda cmd:True))
# Hook into install command late
install.install.sub_commands.append(('install_links', lambda cmd:True))

# check_output was added to subprocess in 2.7
def check_output(args, shell=False):
    pid = sp.Popen(args, stdout=sp.PIPE, shell=shell)
    out, _ = pid.communicate()
    if pid.wait():
        raise RuntimeError('process failed %s %s'%(args,kws))
    return out.decode()

# disable warnings from python 2.x API
extra_cflags=[]
extra_ldflags=[]

import platform
if platform.system() == "Linux":
    extra_cflags.append('-Wno-write-strings')
    if platform.dist()[0].lower() in ["debian", "ubuntu"]:
        try:
            extra_cflags+=check_output('dpkg-buildflags --get CPPFLAGS', shell=True).split()
            extra_cflags+=check_output('dpkg-buildflags --get CFLAGS', shell=True).split()
            extra_ldflags=check_output('dpkg-buildflags --get LDFLAGS', shell=True).split()
        except:
            import traceback
            traceback.print_exc()
    else:
        import os
        extra_cflags+=['-D_FORTIFY_SOURCE=2']
        extra_cflags+=['-g', '-O2', '-fdebug-prefix-map='+os.getcwd()+'=.', '-fstack-protector-strong', '-Wformat', '-Werror=format-security']
        extra_ldflags=['-z', 'relro']

setup(
    name = "carchivetools",
    version = "2.2",
    description = "Tools to query EPICS Channel Archiver and Archiver Appliance and export data",
    long_description = """Tools to retrieve data from EPICS data archivers.
Supports Channel Archiver as well as Archiver Appliance.
Exports of data to Archiver Appliance and H5 are supported.
""",
    url = "https://github.com/epicsdeb/carchivetools",
    download_url = "https://github.com/epicsdeb/carchivetools/releases",
    author = "Michael Davidsaver",
    author_email = "mdavidsaver@bnl.gov",
    license = "BSD",
    packages = ['carchive',
                'carchive.a2aproxy',
                'carchive.a2aproxy.test',
                'carchive.archmiddle',
                'carchive.cmd',
                'carchive.backend',
                'carchive.backend.test',
                'carchive.backend.pb',
               ],
    package_data={'carchive.backend.test': ['*.pb']},
    py_modules = ['twisted.plugins.a2aproxy', 'twisted.plugins.archmiddle'],
    scripts = ['arget','arplothdf5'],
    ext_modules=[Extension('carchive.backend.pbdecode',
                           ['carchive/backend/pbdecode.cpp',
                            'carchive/backend/generated.cpp'],
                           include_dirs=get_numpy_include_dirs(),
                           libraries=['protobuf'],
                           extra_compile_args=extra_cflags,
                           extra_link_args=extra_ldflags,
                 )],

    # local extras and replacements
    cmdclass={
        'build_protobuf':GenProtobuf,
        'build_ext':BuildExtGen,
        'install_links':LinkScripts,
    },

    # local options
    x_proto = (
        ('carchive/backend/EPICSEvent.proto', {'cpp':True, 'py':True}),
    ),
    x_link_scripts = (
        ('arget', 'arinfo'),
        ('arget', 'argrep'),
        ('arget', 'arsnap'),
    )
)
