import io

from setuptools import setup, find_packages

__author__ = 'Memory_Leak<irealing@163.com>'
with io.open("README.md", 'rt', encoding='utf-8') as f:
    readme = f.read()
setup(
    name='red_cache',
    version='0.0.4',
    author='Memory_Leak',
    url="http://vvia.xyz/wjLSh5",
    author_email='irealing@163.com',
    packages=find_packages(),
    install_requires=('redis>=3.0.1',),
    long_description=readme,
)
