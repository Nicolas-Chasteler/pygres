from setuptools import setup, find_packages

setup(
    name='pygres',
    version='0.1',
    description='A simple Python library for running and validating PostgreSQL scripts on startup',
    author='Nicolas Chasteler',
    author_email='nicchasteler@gmail.com',
    packages=find_packages(),
    install_requires=[
        'psycopg2-binary',
    ],
)