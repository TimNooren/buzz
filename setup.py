
import os
from setuptools import setup


try:
    from pypandoc import convert
    README = convert('README.md', 'rst')
except ImportError:
    README = open(os.path.join(os.path.dirname(__file__), 'README.md'), 'r', encoding="utf-8").read()

setup(
    name="bokchoi",
    version="0.2.5",
    packages=['bokchoi'],
    install_requires=[
        'Click',
        'boto3',
        'botocore',
        'paramiko',
        'requests'
    ],
    url='https://github.com/TimNooren/bokchoi',
    author='Tim Nooren',
    author_email='timnooren@gmail.com',
    long_description=README,
    license='MIT',
    entry_points={
        'console_scripts': [
            'bokchoi=bokchoi.cli:cli'
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
    ],
)
