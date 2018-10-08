import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pupyt",
    version="0.0.6",
    author="Danny Beachnau",
    author_email="DannyBeachnau@gmail.com",
    description="pure python table",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Beachnad/pupyt/",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)