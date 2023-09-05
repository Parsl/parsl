from setuptools import setup, find_packages

with open('parsl/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

extras_require = {
    'monitoring' : [
        'sqlalchemy>=1.4,<2',
        'pydot',
        'networkx>=2.5,<2.6',
        'Flask>=1.0.2',
        'flask_sqlalchemy',
        'pandas<2',
        'plotly',
        'python-daemon'
    ],
    'aws' : ['boto3'],
    'kubernetes' : ['kubernetes'],
    'oauth_ssh' : ['oauth-ssh>=0.9'],
    'docs' : ['nbsphinx', 'sphinx_rtd_theme', 'ipython<=8.6.0'],
    'google_cloud' : ['google-auth', 'google-api-python-client'],
    'gssapi' : ['python-gssapi'],
    'azure' : ['azure<=4', 'msrestazure'],
    'workqueue': ['work_queue'],
    'flux': ['pyyaml', 'cffi', 'jsonschema'],
    'proxystore': ['proxystore'],
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
    ],
    keywords=['Workflows', 'Scientific computing'],
    entry_points={'console_scripts':
      [
       'parsl-globus-auth=parsl.data_provider.globus:cli_run',
       'parsl-visualize=parsl.monitoring.visualization.app:cli_run',
       'parsl-perf=parsl.benchmark.perf:cli_run',
      ]}
)
