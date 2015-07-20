try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Utilities to backup and restore using AWS S3',
    'author': 'Ben Jones',
    'url': 'http://github.com/RealSamon/s3backuputils',
    'download_url': 'http://github.com/RealSamon/s3backuputils',
    'author_email': 'ben.jones@superutility.net',
    'version': '0.1',
    'install_requires': ['boto', 'python-dateutil', 'jinja2', 'nose', 'coverage', 'moto', 'tox'],
    'packages': ['s3backuputils'],
    'scripts': ['bin/tar-to-s3', 'bin/untar-from-s3', 'bin/prune-s3-bucket'],
    'name': 's3backuputils'
}

setup(**config)
