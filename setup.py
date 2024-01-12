from setuptools import setup, find_packages

with open('parsl/version.py') as f:
    exec(f.read())

install_requires = [
    "pyzmq>=17.1.2",
    "typeguard>=2.10,<3",
    "typing-extensions>=4.6,<5",
    "six",
    "globus-sdk",
    "dill",
    "tblib",
    "requests",
    "paramiko",
    "psutil>=5.5.1",
    "setproctitle",
]

extras_require = {
    'monitoring': [
        'sqlalchemy>=1.4,<2'
    ],
    'visualization': [
        'pydot',
        'networkx>=2.5,<2.6',
        'Flask>=1.0.2',
        'flask_sqlalchemy',
        'pandas<3',
        'plotly',
        'python-daemon'
    ],
    'aws': ['boto3'],
    'kubernetes': ['kubernetes'],
    'oauth_ssh': ['oauth-ssh>=0.9'],
    'docs': ['nbsphinx', 'sphinx_rtd_theme', 'ipython<=8.6.0'],
    'google_cloud': ['google-auth', 'google-api-python-client'],
    'gssapi': ['python-gssapi'],
    'azure': ['azure<=4', 'msrestazure'],
    'workqueue': ['work_queue'],
    'flux': ['pyyaml', 'cffi', 'jsonschema'],
    'proxystore': ['proxystore'],
    'radical-pilot': ['radical.pilot'],
    'test': [
        'flake8==6.1.0',
        'ipyparallel',
        'pandas',
        'pytest>=7.4.0,<8',
        'pytest-cov',
        'pytest-random-order',
        'mock>=1.0.0',
        'mpi4py',
        'nbsphinx',
        'sphinx_rtd_theme',
        'mypy==1.5.1',
        'types-python-dateutil',
        'types-requests',
        'types-six',
        'types-paramiko',
        'Sphinx==4.5.0',
        'wheel',
    ],

    # Disabling psi-j since github direct links are not allowed by pypi
    # 'psij': ['psi-j-parsl@git+https://github.com/ExaWorks/psi-j-parsl']
}
extras_require['all'] = sum(extras_require.values(), [])

setup(
    name='parsl',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple parallel workflows system for Python',
    url='https://github.com/Parsl/parsl',
    author='The Parsl Team',
    author_email='parsl@googlegroups.com',
    license='Apache 2.0',
    download_url='https://github.com/Parsl/parsl/archive/{}.tar.gz'.format(VERSION),
    include_package_data=True,
    package_data={'parsl': ['py.typed']},
    packages=find_packages(),
    python_requires=">=3.8.0",
    install_requires=install_requires,
    scripts = ['parsl/executors/high_throughput/process_worker_pool.py',
               'parsl/executors/workqueue/exec_parsl_function.py',
               'parsl/executors/workqueue/parsl_coprocess.py',
    ],

    extras_require=extras_require,
    classifiers=[
        # Maturity
        'Development Status :: 5 - Production/Stable',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    keywords=['Workflows', 'Scientific computing'],
    entry_points={'console_scripts':
      [
       'parsl-globus-auth=parsl.data_provider.globus:cli_run',
       'parsl-visualize=parsl.monitoring.visualization.app:cli_run',
       'parsl-perf=parsl.benchmark.perf:cli_run',
      ]}
)
