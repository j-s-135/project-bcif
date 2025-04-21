##
# File:    setup.py
# Author:  James Smith
# Date:    21-Apr-2025
##

"""
Project dependencies.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

from setuptools import setup, find_packages

setup(
    name="bcif",
    version=1.0,
    install_requires=[
        "hydra-core>=1.3.2",
        "apache-airflow>=2.10.3"
    ],
    packages=find_packages()
)
