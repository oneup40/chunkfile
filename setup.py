from setuptools import setup
import os

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), 'r') as f:
    long_description = f.read()

setup(
	name='chunkfile',
	version='0.0a0.dev0',
	description='A file-like interface backed by multiple smaller files.',
	url='https://github.com/oneup40/chunkfile',
	author='oneup40',
	author_email='oneup40@gmail.com',
	license='Free for non-commercial use',
	classifiers=[
		'Development Status :: 1 - Planning',
		'Intended Audience :: Developers',
		'License :: Free for non-commercial use',
		'Operating System :: Microsoft :: Windows :: Windows 10',
		'Programming Language :: Python :: 2.6',
		'Programming Language :: Python :: 2.7',
		'Topic :: Software Development :: Libraries',
		'Topic :: System :: Filesystems',
	],
	keywords='chunk file filesystem',
	packages=['chunkfile'],
	install_requires=['pathlib'],
	extras_require={},
	package_data={},
)
