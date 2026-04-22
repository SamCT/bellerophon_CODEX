import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bellerophon",
    author="Dave Bouvier",
    author_email="dave@bx.psu.edu",
    description="Filter reads of a minimum quality that span a junction, retaining the 5´ side of that junction",
    install_requires=['pysam>=0.22'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    test_suite='nose.collector',
    tests_require=['nose'],
    url="https://github.com/davebx/bellerophon",
    packages=['bellerophon'],
    entry_points={'console_scripts': ['bellerophon=bellerophon.cli:main']},
    classifiers=[
        "Programming Language :: Python :: 3",
        'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
)
