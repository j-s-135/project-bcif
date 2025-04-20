from setuptools import setup, find_packages

setup(
    name="bcif",
    version=1.0,
    install_requires=[
        "hydra-core>=1.3.2",
        "mmcif",
        "requests"
    ],
    packages=find_packages()
)

