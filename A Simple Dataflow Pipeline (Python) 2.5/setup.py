# setup.py
import setuptools

setuptools.setup(
    name='my-dataflow-job',
    version='1.0.0',
    install_requires=['apache-beam[gcp]==2.66.0'],  # Use a recent version here
    packages=setuptools.find_packages()
)