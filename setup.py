from setuptools import setup

setup(
    name='graph_comp',
    packages=['graph_comp'],
    version='0.2.0',
    description='Simple computation graph library with cost support.',
    author='Brian Guarraci',
    license='MIT',
    setup_requires=['pytest-runner'],
    tests_require=['pytest==6.2.1'],
    test_suite='tests',
)
