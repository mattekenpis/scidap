from setuptools import setup, find_packages
import setuptools.command.egg_info as egg_info_cmd

import sys

try:
    import gittaggers
    tagger = gittaggers.EggInfoFromGit
except ImportError:
    tagger = egg_info_cmd.egg_info

version = '0.0.1a'

mysql = ['mysql-python>=1.2.5']

#all_dbs = postgres + mysql + hive + mssql + hdfs
#devel = all_dbs + doc + samba + s3 + ['nose'] + slack + crypto + oracle

setup(
    name='scidap',
    description='Scientific Data Analysis Platform',
    version=version,
    packages=find_packages(),
    #packages=["scidap"],
    zip_safe=False,
    install_requires=[
        'airflow',
        'cwltool'
    ],
    extras_require={
        'mysql': mysql
    },
    author='Andrey Kartashov',
    author_email='porter@scidap.com',
    url='https://github.com/scidap/scidap',
    download_url=(
        'https://github.com/scidap/scidap/tarball/' + version),
    cmdclass={'egg_info': tagger},        
)
