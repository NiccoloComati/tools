from setuptools import setup, find_packages

setup(
    name="data_tools",                 # <- Python package name
    version="0.1",
    packages=find_packages(),         # <- includes data_tools/
    install_requires=[
        "boto3",
        "tqdm"
    ],
)