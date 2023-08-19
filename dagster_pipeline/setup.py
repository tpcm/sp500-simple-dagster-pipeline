from setuptools import find_packages, setup

setup(
    name="my_dagster_project",
    packages=find_packages(exclude=["my_dagster_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "yfinance",
        "pyarrow",
        "azure-storage-blob",
        
        

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
