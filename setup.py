from setuptools import setup, find_packages
from parsl.version import VERSION

with open('requirements.txt') as f:
    install_requires = f.readlines()

# tests_require = parse_requirements('test-requirements.txt')

setup(
    name='parsl',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple and easy parallel workflows system for Python',
    url='https://github.com/Parsl/parsl',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    download_url='https://github.com/Parsl/parsl/archive/{}.tar.gz'.format(VERSION),
    package_data={'': ['LICENSE']},
    packages=find_packages(),
    install_requires=install_requires,
    classifiers=[
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
    keywords=['Workflows', 'Scientific computing'],
)
