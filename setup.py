try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Utilities to backup and restore using AWS S3',
    'author': 'Ben Jones',
    'url': 'http://gitlab.com/RealSamon/s3backuputils',
    'download_url': 'http://gitlab.com/RealSamon/s3backuputils',
    'author_email': 'ben.jones@superutility.net',
    'version': '0.1',
    'install_requires': ['boto', 'python-dateutil', 'nose', 'moto', 'tox'],
    'packages': ['s3backuputils'],
    #'scripts': ['bin/s3-backup', 'bin/s3-restore'],
    'name': 's3backuputils'
}

setup(**config)
