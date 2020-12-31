from setuptools import setup

setup(
    name='graph_flow',
    packages=['graph_flow'],
    version='0.1.0',
    description='Cost based graph data flow library',
    author='Brian Guarraci',
    license='MIT',
    setup_requires=['pytest-runner'],
    tests_require=['pytest==6.2.1'],
    test_suite='tests',
)
