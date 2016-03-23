from setuptools import setup, find_packages

with open("README.rst") as f:
	readme = f.read()

with open("LICENSE") as f:
	license = f.read()

setup(
	name="gdo",
	version="0.1.0",
	description="Concurrent execution library",
	long_description=readme,
	author="Muhammed Uluyol",
	author_email="uluyol0@gmail.com",
	url="https://github.com/uluyol/gdo",
	license=license,
	packages=find_packages(exclude=("tests", "docs")),
	test_suite="tests",
)
