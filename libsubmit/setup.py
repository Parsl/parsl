from setuptools import setup, find_packages
from libsubmit.version import VERSION

install_requires = [
    'ipyparallel',
    'paramiko'
    ]

tests_require = [
    'ipyparallel',
    'paramiko',
    'mock>=1.0.0',
    'nose',
    'pytest'
    ]

setup(
    name='libsubmit',
    version=VERSION,
    description='Uniform interface to clouds, clusters, grids and supercomputers.',
    long_description='Submit, track and cancel arbitrary bash scripts on computate resources',
    url='https://github.com/Parsl/libsubmit',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    download_url = 'https://github.com/Parsl/libsubmit/archive/master.zip',
    package_data={'': ['LICENSE']},
    packages=find_packages(),
    install_requires=install_requires,
    extras_require = {
        'aws' : ['boto3'],
        'azure' : ['azure', 'haikunator'],
        'jetstream' : ['python-novaclient']
        },
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
)
