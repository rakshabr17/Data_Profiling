from setuptools import setup, find_packages

setup(
    name="dataprofiling_package",
    version="0.1.0",
    description="A package for fetching and comparing SQL statistics",
    author="Raksha",
    author_email="raksha.kanguvalli@ascendion.com",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'sqlalchemy',
        'pandas',
        'pyodbc',
    ],
    include_package_data=True,
    package_data={
        'dataprofiling_package': ['../configuration/*.json'],
    },
    entry_points={
        'console_scripts': [
            'fetch-stats=dataprofiling_package.stats_from_sql7:main',
            'compare-stats=dataprofiling_package.compare_args:main',
        ],
    },
)