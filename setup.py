from setuptools import setup

# Read requirements
with open('requirements.txt', 'r') as fh:
    reqs = [str(x).strip() for x in fh.readlines()]

# Read version string
with open('pyarrow_utils/_version.py', 'r') as fh:
    for line in fh:
        if line.startswith('__version__'):
            exec(line)

setup(
    name="pyarrow_utils",
    version=__version__,
    author='Aaron Dallas',
    description='PyArrow Parquet Utilities',
    url='https://github.com/aarondallas/PyArrowUtils',
    packages=['pyarrow_utils'],
    install_requires=reqs,
)

