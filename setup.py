from setuptools import setup
from parsl.version import VERSION

install_requires = [
    ]

tests_require = [
    'mock>=1.0.0',
    'nose',
    'pytest'
    ]

setup(
    name='parsl',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple and easy parallel workflows system for Python',
    url='https://github.com/swift-lang/swift-e-lab',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    download_url = 'https://github.com/swift-lang/swift-e-lab/archive/0.1.tar.gz',
    keywords = ['Workflows', 'Scientific computing'],
    package_data={'': ['LICENSE']},
    packages=['parsl', 'parsl.app', 'parsl.dataflow'],
    install_requires=install_requires,
    #tests_require=tests_require
)
