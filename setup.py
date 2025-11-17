from setuptools import setup, find_packages

setup(
    name="tgms-worker",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy==2.0.23",
        "psycopg2-binary==2.9.9",
        "python-dotenv==1.0.0",
        "requests==2.31.0",
        "aiohttp==3.9.1"
    ]
)