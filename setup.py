from setuptools import setup, find_packages

with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

setup(
    name='cetino',
    version='0.1.2',
    packages=find_packages(),
    install_requires=requirements,
    author='steveflyer',
    author_email='steveflyer7@gmail.com',
    description='Store and load your data in a unified and light-weight way.',
    license='MIT',
    keywords='sqlite dict storage',
    url='https://github.com/stevieflyer/cetino.git'
)
