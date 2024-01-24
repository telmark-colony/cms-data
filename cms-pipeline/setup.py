from setuptools import find_packages, setup

setup(
    name="cms_pipeline",
    packages=find_packages(exclude=["cms_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-gcp",
        "dagster-gcp-pandas",
        "google-cloud-storage",
        "openpyxl"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
