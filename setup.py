import ast
from setuptools import setup, find_packages


def find_version(filename):
    with open(filename) as f:
        initlines = f.readlines()
    version_line = None
    for line in initlines:
        if line.startswith('__version__'):
            vstr = line.strip().split()[-1]
            ver = ast.literal_eval(vstr)
            break


setup(
    name='xpdan',
    version=find_version('xpdan/__init__.py'),
    packages=find_packages(),
    description='data processing module',
    zip_safe=False,
    package_data={'xpdan': ['config/*']},
    include_package_data=True,
    url='http:/github.com/xpdAcq/xpdAn'
)
