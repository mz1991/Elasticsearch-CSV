try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'ElasticSearch Upload',
    'author': 'Matteo Zuccon',
    'url': '',
    'download_url': '',
    'author_email': 'm.zuccon@campus.unimib.it',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['NAME'],
    'scripts': [],
    'name': 'ElasticSearch Upload'
}

setup(**config)