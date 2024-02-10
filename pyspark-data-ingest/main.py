from src.data_reader.data_reader import read_data, main as data_reader_main

if __name__ == "__main__":
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