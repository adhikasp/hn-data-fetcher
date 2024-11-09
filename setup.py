from setuptools import setup, find_packages

setup(
    name="hn_async",
    version="1.0.3",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "aiohttp",
        "tqdm",
    ],
    python_requires=">=3.8",
) 