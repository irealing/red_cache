from setuptools import setup, find_packages

__author__ = 'Memory_Leak<irealing@163.com>'

setup(
    name='red_cache',
    version='0.0.1',
    author='Memory_Leak',
    author_email='irealing@163.com',
    packages=find_packages(),
    install_requires=('redis>=3.0.1',)
)
