import io

from setuptools import setup, find_packages

__author__ = 'Memory_Leak<irealing@163.com>'
with io.open("README.md", 'rt', encoding='utf-8') as f:
    readme = f.read()
setup(
    name='red_cache',
    version='0.2.3',
    author='Memory_Leak',
    url="https://gitee.com/irealing/red_cache",
    author_email='irealing@163.com',
    packages=find_packages(),
    install_requires=('redis>=3.0.1',),
    long_description=readme,
    long_description_content_type='text/markdown',
)
