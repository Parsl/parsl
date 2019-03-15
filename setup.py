from setuptools import setup, find_packages

with open('viz_server/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='viz_server',
    version=VERSION,
    description='Visualizing data dependent workflows in Python',
    long_description='Visualizing parallel workflows system for Python',
    url='https://github.com/Parsl/viz_server',
    author='The Parsl Team',
    author_email='parsl@googlegroups.com',
    license='Apache 2.0',
    download_url='https://github.com/Parsl/parsl/archive/{}.tar.gz'.format(VERSION),
    include_package_data=True,
    packages=find_packages(),
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
    keywords=['Visualization', 'Workflows', 'Scientific computing'],
    entry_points={'console_scripts':
      ['parsl-visualize=viz_server.viz:cli_run',
      ]}
)
