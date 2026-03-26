from setuptools import setup, find_packages

setup(
    name="aws-util",
    version="0.1.0",
    author="Masrik Dahir",
    author_email="your.email@example.com",
    description="A utility library for AWS services.",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "boto3",  # Example dependency
        # Add other dependencies as needed
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Minimum Python version
)
