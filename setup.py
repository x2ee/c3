from setuptools import find_packages, setup


# MANIFEST.in ensures that readme and version included into sdist

install_requires = [
    "pandas",
    "numpy",
    "pyarrow",
    "duckdb",
    "pydantic",
    "croniter",
    "cryptography",
    "tornado"
]
dev_requires = [
    "mypy",
    "coverage",
    "types-croniter",
    "wheel",
    "twine",
    "black",
    "isort",
    "pytest<=8.0.0",
    "pytest-mypy",
    "pytest-cov",
]


def read_file(f):
    with open(f, "r") as fh:
        return fh.read()


long_description = read_file("README.md")


setup(
    name="x2c3",
    version='0.0.1',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Data Scientists",
        "Topic :: System :: Compute Engine",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    description="compute cache cron",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/x2ee/c3",
    author="Walnut Geek",
    author_email="wg@walnutgeek.com",
    license="Apache 2.0",
    packages=find_packages(exclude=("*.tests",)),
    # package_data={"files": ["mime_infos.json"]},
    entry_points={"console_scripts": ["c3=x2.c3.cli:main"]},
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
    zip_safe=False,
)
