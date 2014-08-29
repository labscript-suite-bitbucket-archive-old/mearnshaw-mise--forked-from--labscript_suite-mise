from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages
setup(
        name = "mise",
        include_package_data = True,
        packages = find_packages(),
        install_requires = ['zprocess']
)

