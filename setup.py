# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""SQLAlchemy CRUD Utilities"""

import os
from setuptools import setup, find_packages


# Package versioning solution originally found here:
# http://stackoverflow.com/q/458550
exec(open(os.path.join('residue', '_version.py')).read())

reqs = open('requirements.txt', 'r').read().strip().splitlines()

setup(
    name='residue',
    version=__version__,
    url='http://residue.readthedocs.org',
    download_url='http://pypi.python.org/pypi/residue',
    license='BSD',
    author='Rob Ruana',
    author_email='rob@robruana.com',
    description=__doc__,
    long_description=open('README.rst', 'r').read(),
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Utilities',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    platforms='any',
    packages=find_packages(exclude=['tests*']),
    install_requires=reqs
)
