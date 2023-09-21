import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='wcs-remote-service',
    packages=find_packages(where='src'),
    package_dir={'':'src'},
    version='1.0.0',
    include_package_data=True,
    license='GPL',
    description='GeoNode WCS remote service',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://52north.org',
    author='52North Developers',
    author_email='info@52north.org',
    install_requires=[
        # 'django',
        # 'geonode',
        'owslib'
    ],
)