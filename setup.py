from setuptools import setup, find_packages

setup(
    name='gbdx-buffet',
    version='2018.03.13.0',
    description='A package to simplify basic AOP based GBDX ordering through a Python CLI',
    url='https://github.com/digitalglobe/gbdx-buffet',
    author='Mahmoud Lababidi',
    author_email='mla@mla.im',
    license='Apache',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=['sh', 'regex'],
    python_requires='>=3',
    entry_points={
        'console_scripts': [
            'buffet-dl=gbdx_buffet:download_cli',
            'buffet-order=gbdx_buffet:workflow_cli',
            'buffet-results=gbdx_buffet:fetch_results_cli',
            'buffet-status=gbdx_buffet:check_workflow_cli',
        ],
    },
)
