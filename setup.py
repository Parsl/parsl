from setuptools import setup
from parsl.version import VERSION

setup(
    name='parsl',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple and easy parallel workflows system for Python',
    url='https://github.com/swift-lang/swift-e-lab',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    package_data={'': ['LICENSE']},
    packages=['parsl', 'parsl.app', 'parsl.dataflow'],
    install_requires=[],
)
