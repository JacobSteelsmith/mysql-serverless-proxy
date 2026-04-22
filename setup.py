"""Setup script for MySQL-to-RDS Data API proxy."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="mysql-rds-proxy",
    version="0.1.0",
    author="MySQL RDS Proxy Team",
    description="A local proxy server that translates MySQL protocol to AWS RDS Data API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.10",
    install_requires=[
        "pyyaml>=6.0",
        "boto3>=1.26.0",
        "mysql-mimic>=2.5.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "hypothesis>=6.0.0",
            "pytest-cov>=4.0.0",
            "moto>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "mysql-rds-proxy=mysql_rds_proxy.cli:main",
        ],
    },
)
