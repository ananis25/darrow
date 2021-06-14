import setuptools

with open("../README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="arrowpy", # Replace with your own username
    version="0.0.1",
    author="author",
    author_email="author@example.com",
    description="Helpers for Arrow datasets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/silverleafcaps/darrow",
    packages=['arrowpy'],
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Linux",
    ],
    python_requires='>=3.7',
)
