from setuptools import setup
from parsl.version import VERSION

install_requires = [
    'ipyparallel',
    'libsubmit>=0.2.3'
    ]

tests_require = [
    'ipyparallel',
    'mock>=1.0.0',
    'nose',
    'pytest'
    ]

setup(
    name='parsl',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple and easy parallel workflows system for Python',
    url='https://github.com/Parsl/parsl',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    download_url = 'https://github.com/Parsl/parsl/archive/0.2.1.tar.gz',
    package_data={'': ['LICENSE']},
    packages=['parsl', 'parsl.app', 'parsl.dataflow', 'parsl.executors',
              'parsl.execution_provider', 'parsl.data_provider'],
    install_requires=install_requires,
    classifiers = [
        # Maturity
        'Development Status :: 3 - Alpha',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords = ['Workflows', 'Scientific computing'],
    #tests_require=tests_require
)
