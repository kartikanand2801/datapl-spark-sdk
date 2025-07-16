from setuptools import setup, find_packages

setup(
    name="spark-table-sdk",
    version="0.1.0",
    description="SDK to write Spark DataFrames as Delta/Parquet tables with partitioning and catalog support",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.1.2",
    ],
    python_requires=">=3.7",
)
