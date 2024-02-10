from pyspark.sql import SparkSession

def read_data(spark: SparkSession, data_source: str, path: str, format: str = None, options: dict = None):
    """
    Read data from different data sources.
    :param spark: SparkSession object.
    :param data_source: Data source type (csv, json, parquet, etc.).
    :param path: Path to the data file or directory.
    :param format: File format (csv, json, parquet, etc.).
    :param options: Additional options for reading data.
    :return: DataFrame object.
    """
    if format is None:
        format = data_source
    reader = spark.read.format(format)
    if options is not None:
        for key, value in options.items():
            reader = reader.option(key, value)
    return reader.load(path)

def main():
    # Create a SparkSession object
    spark = SparkSession.builder.appName("Reading Different Data Sources").getOrCreate()

    # Reading CSV data
    csv_data = read_data(spark, "csv", "path/to/csv_file.csv")
    csv_data.show()

    # Reading JSON data
    json_data = read_data(spark, "json", "path/to/json_file.json")
    json_data.show()

    # Reading Parquet data
    parquet_data = read_data(spark, "parquet", "path/to/parquet_file.parquet")
    parquet_data.show()

    # Reading data from a directory
    directory_data = read_data(spark, "parquet", "path/to/directory/with/parquet_files")
    directory_data.show()

    # Reading data with options
    options = {"header": "true", "inferSchema": "true"}
    csv_with_options_data = read_data(spark, "csv", "path/to/csv_file_with_header.csv", options=options)
    csv_with_options_data.show()

    spark.stop()