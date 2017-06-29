#!/usr/bin/env python
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


version = ''
with open('tablestore/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


with open('README.rst', 'rb') as f:
    readme = f.read().decode('utf-8')

setup(
    name='tablestore',
    version=version,
    description='Aliyun TableStore(OTS) SDK',
    long_description=readme,
    packages=['tablestore', 'tablestore.protobuf', 'tablestore.plainbuffer'],
    install_requires=['protobuf-py3>=2.5.1', 'urllib3>=1.14', 'certifi>=2016.2.28'],
    include_package_data=True,
    url='https://cn.aliyun.com/product/ots',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ]
)

