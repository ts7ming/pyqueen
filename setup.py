from setuptools import setup, find_packages

setup(
    name='pyqueen',
    version='1.1.4',
    url='https://github.com/ts7ming/pyqueen.git',
    description='Rule your Data',
    long_description=open("README.md", encoding='utf-8').read(),
    long_description_content_type="text/markdown",
    author='ts7ming',
    author_email='qiming.ma@outlook.com',
    packages=find_packages(),
    python_requires=">=3.4",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pandas>=2.0.0',
        'SQLAlchemy>=2.0.0',
        'xlrd==1.2.0',
        'XlsxWriter'
    ],
    entry_points={
        'console_scripts': [
            'pyqueen = pyqueen:cmd',
        ]
    }
)
