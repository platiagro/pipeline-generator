# -*- coding: utf-8 -*-
import re
from os.path import dirname, join
from setuptools import find_packages, setup

with open(join(dirname(__file__), 'pipelines', '__init__.py')) as fp:
    for line in fp:
        m = re.search(r'^\s*__version__\s*=\s*([\'"])([^\'"]+)\1\s*$', line)
        if m:
            version = m.group(2)
            break
    else:
        raise RuntimeError('Unable to find own __version__ string')

def get_requirements(reqfile):
    with open(reqfile) as f:
        return f.read().splitlines()


extras = {
    "testing": [
        "pytest>=4.4.0",
        "pytest-xdist==1.31.0",
        "pytest-cov==2.8.1",
        "flake8==3.7.9",
    ]
}

setup(
    name='pipelines',
    version=version,
    description='Manage pipelines.',
    license='Apache License 2.0',

    packages=find_packages(),
    install_requires=get_requirements('requirements.txt'),
    extras_require=extras,

    author='Miguel Figueira Ferraz',
    author_email='mferraz@cpqd.com.br',
    url='https://github.com/platiagro/pipelines',
)
