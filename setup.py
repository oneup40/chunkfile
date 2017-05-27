from setuptools import setup
import os

here = os.path.abspath(os.path.dirname(__file__))

setup(
	name='chunkfile',
	version='1.0.0-b2',
	description='A file-like interface backed by multiple smaller files.',
	url='https://github.com/oneup40/chunkfile',
	download_url='https://github.com/oneup40/chunkfile/archive/v1.0.0-b2.tar.gz',
	author='oneup40',
	author_email='oneup40@gmail.com',
	license='Free for non-commercial use',
	classifiers=[
		'Development Status :: 4 - Beta',
		'Intended Audience :: Developers',
		'License :: Free for non-commercial use',
		'Operating System :: POSIX :: Linux',
		'Programming Language :: Python :: 2.6',
		'Programming Language :: Python :: 2.7',
		'Programming Language :: Python :: 3.3',
		'Programming Language :: Python :: 3.4',
		'Programming Language :: Python :: 3.5',
		'Programming Language :: Python :: 3.6',
		'Topic :: Software Development :: Libraries',
		'Topic :: System :: Filesystems',
	],
	keywords='chunk file filesystem',
	packages=['chunkfile'],
	install_requires=['pathlib'],
	extras_require={},
	package_data={},
)
