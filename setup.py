from setuptools import setup, find_packages

setup(
	name="gdo",
	version="0.1.2",
	description="Concurrent execution library",
	long_description="",
	author="Muhammed Uluyol",
	author_email="uluyol0@gmail.com",
	url="https://github.com/uluyol/gdo",
	license="MIT",
	packages=find_packages(exclude=("tests", "docs")),
	install_requires=[
		"gevent",
		"gipc",
	],
	test_suite="tests",
)
