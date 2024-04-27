from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import datetime
import datavault_dlt_databricks

setup(
    name="datavault_dlt_databricks",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=datavault_dlt_databricks.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    description="wheel file based on datavault_dlt_databricks/src",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        "packages": [
            "main=datavault_dlt_databricks.main:main"
        ]
    },
    install_requires=[
        "setuptools"
    ],
)
