from setuptools import find_packages, setup

setup(
    name='graph_flow',
    packages=find_packages(include='graph_flow'),
    version='0.1.0',
    description='cost based graph data flow library',
    author='Brian Guarraci',
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)
