PySpark Data Reader
This project demonstrates how to read data from different data sources using PySpark. It includes modular code for reading CSV, JSON, Parquet, and directory data sources.

Installation
Install PySpark: https://spark.apache.org/docs/latest/api/python/getting_started/install.html

Install the project:

$ pip install .
Usage
Run the project:

$ my_project
This will read data from different data sources and display the results.

Project Structure
The project structure is designed to keep the code modular and organized. Here is an explanation of each file and directory in the project:

README.md: This file contains documentation about the project, including how to install and use it.
requirements.txt: This file lists the necessary dependencies for the project, which can be installed using pip install -r requirements.txt. In this case, the only dependency is PySpark.
setup.py: This file contains the necessary setup information for the project, including the name, version, packages, and dependencies. It also includes an entry point for the main function, which allows the project to be run as a command-line script using my_project.
main.py: This file is the entry point of the application. It imports the necessary modules and calls the main function. It demonstrates how to use the read_data function to read data from different data sources.
src: This directory contains the source code for the project.
src/data_reader: This directory contains the data_reader.py file, which contains the read_data function and any other utility functions related to reading data.
src/data_reader/**main**.py: This file allows the main function in data_reader.py to be run directly using python -m src.data_reader.
Code Description
The read_data function in data_reader.py reads data from different data sources. It takes the SparkSession object, data source type, path to the data file or directory, file format, and additional options for reading data as parameters. The function returns a DataFrame object.

The main function in main.py demonstrates how to use the read_data function to read data from different data sources. It reads CSV, JSON, and Parquet data, as well as data from a directory. It also shows how to read data with options.

Running the Code
To run the code, you need to replace the paths with your actual paths to the data files or directories. You can run the project using the following command:

$ my_project
Alternatively, you can run the main function in data_reader.py directly using the following command:

$ python -m src.data_reader
Testing
To test the code, you can use PySpark's built-in testing framework. You can create test cases for each data source and verify that the data is read correctly.

Contributing
We welcome contributions to the project. If you want to contribute, please fork the repository and submit a pull request.

License
This project is licensed under the MIT License.
