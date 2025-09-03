import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

__version__ = "1.4.4"

setuptools.setup(
    name="django-bulk-load",
    version=__version__,
    author="Cedar",
    author_email="support@cedar.com",
    license="MIT",
    description="Bulk load Django models",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cedar-team/django-bulk-load",
    project_urls={},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]
    ),
    python_requires=">=3.6",
    install_requires=[
        "django>=2.2",
    ],
    extras_require={
        "psycopg2": ["psycopg2>=2.8.6"],
        "psycopg2-binary": ["psycopg2-binary>=2.8.6"],
    },
)
