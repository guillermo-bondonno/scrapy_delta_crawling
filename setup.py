from setuptools import setup, find_packages

setup(
    name='scrapy_delta_crawling',
    version='0.1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        'scrapy',
        'scrapinghub',
    ],
)
