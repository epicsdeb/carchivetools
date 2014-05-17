#!/usr/bin/env python

from distutils.core import setup, Distribution, Extension, Command, DistutilsSetupError
from distutils.command import build, build_ext, install

from numpy.distutils.misc_util import get_numpy_include_dirs

class GenProtobuf(Command):
    """Run protoc code generator
    """
    user_options = [
        ('protoc=', 'P', "protobuf compiler"),
    ]

    def initialize_options(self):
        self.protoc = 'protoc'
        self.proto = None
        self.build_temp = None
        self.build_lib = None

    def finalize_options(self):
        self.proto = self.distribution.x_proto or ()

        self.set_undefined_options('build',
                                   ('build_temp', 'build_temp'),
                                   ('build_lib', 'build_lib'),
                                  )

        from os.path import isfile
        if self.protoc and not isfile(self.protoc):
            from distutils.spawn import find_executable
            self.protoc = find_executable(self.protoc)
        if not self.protoc:
            raise DistutilsSetupError("Unable to find 'protoc'")

    def run(self):
        self.mkpath(self.build_temp)
        self.mkpath(self.build_lib)

        for pbd, opts in self.proto:
            if opts.get('py', False):
                self.spawn([self.protoc, '--python_out='+self.build_lib, pbd])
            if opts.get('cpp', False):
                self.spawn([self.protoc, '--cpp_out='+self.build_temp, pbd])

class BuildExtGen(build_ext.build_ext):
    """Extend -I search path to find generated files
    """
    def finalize_options(self):
        build_ext.build_ext.finalize_options(self)
        # Search for generated headers referenced from top level (eg. carchive/backend/...)
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

setup(
    name = "carchivetools",
    version = "1.9-dev",
    description = "Tools to query EPICS Channel Archiver and Archiver Appliance",
    long_description = """Tools to retrieve data from EPICS data archivers.
Support Channel Archiver as well as Archiver Appliance.
""",
    url = "https://github.com/epicsdeb/carchivetools",
    download_url = "https://github.com/epicsdeb/carchivetools/releases",
    author = "Michael Davidsaver",
    author_email = "mdavidsaver@bnl.gov",
    license = "BSD",
    packages = ['carchive', 'carchive.cmd', 'carchive.backend'],
    scripts = ['arget','arplothdf5'],
    ext_modules=[Extension('carchive.backend.pbdecode',
                           ['carchive/backend/pbdecode.cpp',
                            'carchive/backend/generated.cpp'],
                           include_dirs=get_numpy_include_dirs(),
                           libraries=['protobuf'],
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
    )
)
