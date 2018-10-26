import os
import sys
import subprocess

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as build_ext_orig
from multiprocessing import cpu_count


if sys.version_info < (3, 6):
    raise RuntimeError('Require Python 3.6 or greater')


PACKAGE_SRC_DIR = 'connectors/py_reindexer/'

PACKAGE_VERSION = '0.0.1'
PACKAGE_NAME = 'pyreindexer'

PACKAGE_URL = 'https://github.com/Restream/reindexer'

PACKAGE_DESCRIPTION = 'A connector that allows to interact with Reindexer'
PACKAGE_LONG_DESCRIPTION = ''


class CMakeExtension(Extension):
    def __init__(self, name):
        super().__init__(name, sources=[])


class build_ext(build_ext_orig):
    def run(self):
        for ext in self.extensions:
            self.build_cmake(ext)

    def build_cmake(self, ext):
        cwd = os.path.abspath('')

        build_temp = os.path.abspath(self.build_temp)
        if not os.path.exists(build_temp):
            os.makedirs(build_temp)

        extension_dir = os.path.abspath(self.get_ext_fullpath(ext.name))
        if not os.path.exists(extension_dir):
            os.makedirs(extension_dir)

        lib_dir = os.path.join(extension_dir, '..')

        os.chdir(build_temp)

        self.spawn(['sh', os.path.join(cwd, 'dependencies.sh')])
        self.spawn(['cmake', cwd, '-DCMAKE_BUILD_TYPE=Release',
                    '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + lib_dir])
        if not self.dry_run:
            self.spawn(['cmake', '--build', '.',
                        '--', '-C' + PACKAGE_SRC_DIR, '-j' + str(cpu_count())])

        os.chdir(cwd)


setup(name=PACKAGE_NAME,
      version=PACKAGE_VERSION,
      description=PACKAGE_DESCRIPTION,
      long_description=PACKAGE_LONG_DESCRIPTION,
      url=PACKAGE_URL,
      author='Igor Tulmentyev',
      author_email='igtulm@gmail.com',
      license='MIT',
      packages=[PACKAGE_NAME],
      package_dir={PACKAGE_NAME: os.path.join(PACKAGE_SRC_DIR, PACKAGE_NAME)},
      ext_modules=[CMakeExtension('rawpyreindexer')],
      cmdclass={
          'build_ext': build_ext,
      },
      test_suite='connectors.py_reindexer.tests',
      )
