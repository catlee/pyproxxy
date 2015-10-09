from setuptools import setup, find_packages


setup(
    name="pyproxxy",
    version="0.1",
    author="Chris AtLee",
    author_email="catlee@mozilla.com",
    scripts=['proxxy.py'],
    url="https://github.com/catlee/pyproxxy",
    license="MPL 2.0",
    description="S3 backed HTTP cache",
    install_requires=[
        'boto3',
        'aiohttp',
    ],
    long_description=open('README.md').read(),
)
