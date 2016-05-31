""" Setup file """
import os
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))


def load(filename):
    return open(os.path.join(HERE, filename))


def get_readme():
    with load("README.rst") as f:
        return f.read()


def get_version():
    with load("dvox/__init__.py") as f:
        for line in f:
            if line.startswith("__version__"):
                return eval(line.split("=")[-1])


def get_requirements():
    with load("requirements.txt") as f:
        return [line.strip() for line in f.readlines()]


if __name__ == "__main__":
    setup(
        name="dvox",
        version=get_version(),
        description="Distributed Voxel Engine",
        long_description=get_readme(),
        classifiers=[
            "Development Status :: 2 - Pre-Alpha",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent"
        ],
        author="Joe Cross",
        author_email="joe.mcross@gmail.com",
        url="https://github.com/numberoverzero/dvox",
        license="MIT",
        keywords="distributed voxel engine",
        platforms="any",
        include_package_data=True,
        packages=find_packages(exclude=("tests", "docs", "examples")),
        install_requires=get_requirements(),
    )