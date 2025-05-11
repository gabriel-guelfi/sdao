from setuptools import setup, find_packages

setup(
    name="sdao",
    version="1.2.2",
    description="A DAO (Data Access Object) library for Python",
    author="Gabriel Valentoni Guelfi",
    author_email="gabriel.valguelfi@gmail.com",
    packages=find_packages(),  # Encontra sdao/
    python_requires=">=3.7",
    install_requires=[],
    extras_require={
        "mysql": ["mysql-connector-python"],
        "mariadb": ["mariadb"],
        "mssql": ["pyodbc"]
    },
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
