from setuptools import setup, find_packages

with open('parsl/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

extras_require = {
    'monitoring' : [
        'sqlalchemy>=1.3.0,!=1.3.4',
        'sqlalchemy_utils',
        'pydot',
        'networkx',
        'Flask>=1.0.2',
        'flask_sqlalchemy',
        'pandas',
        'plotly',
        'python-daemon'
    ],
    'aws' : ['boto3'],
    'kubernetes' : ['kubernetes'],
    'oauth_ssh' : ['oauth-ssh>=0.9'],
    'extreme_scale' : ['mpi4py'],
    'docs' : ['nbsphinx', 'sphinx_rtd_theme', 'ipython'],
    'google_cloud' : ['google-auth', 'google-api-python-client'],
    'gssapi' : ['python-gssapi'],
    'azure' : ['azure<=4', 'msrestazure'],
    'workqueue': ['work_queue'],
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
    packages=find_packages(),
    python_requires=">=3.6.0",
    install_requires=install_requires,
    scripts = ['parsl/executors/high_throughput/process_worker_pool.py',
               'parsl/executors/extreme_scale/mpi_worker_pool.py',
               'parsl/executors/low_latency/lowlatency_worker.py',
               'parsl/executors/workqueue/exec_parsl_function.py',
    ],

    extras_require=extras_require,
    classifiers=[
        # Maturity
        'Development Status :: 3 - Alpha',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords=['Workflows', 'Scientific computing'],
    entry_points={'console_scripts':
      [
       'parsl-globus-auth=parsl.data_provider.globus:cli_run',
       'parsl-visualize=parsl.monitoring.visualization.app:cli_run',
      ]}
)
