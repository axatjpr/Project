import pathlib
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Dict, List

from setup_storage.cloud_env_setup import main as cloud_env_setup_main


def read_local_data(
    spark: SparkSession,
    data_source: str,
    path: pathlib.Path,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read data from local data sources.
    :param spark: SparkSession object.
    :param data_source: Data source type (csv, json, parquet, etc.).
    :param path: Path to the data file or directory.
    :param format: File format (csv, json, parquet, etc.).
       :param options: Additional options for reading data.
    :return: DataFrame object.
    """
    if format is None:
        format = data_source

    reader = spark.read.format(data_source)
    if options is not None:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(str(path))


def read_cloud_data(
    spark: SparkSession,
    data_source: str,
    path: pathlib.Path,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read data from cloud data sources.
    :param spark: SparkSession object.
    :param data_source: Data source type (csv, json, parquet, etc.).
    :param path: Path to the data file or directory.
    :param format: File format (csv, json, parquet, etc.).
       :param options: Additional options for reading data.
    :return: DataFrame object.
    """
    if format is None:
        format = data_source

    reader = spark.read.format(data_source)
    if options is not None:
        for key, value in options.items():
            reader = reader.option(key, value)

    if str(path).startswith("s3a://"):
        return reader.load(path.as_uri())
    elif str(path).startswith("wasbs://"):
        return reader.load(path.as_uri())
    elif str(path).startswith("gs://"):
        return reader.load(path.as_uri())
    else:
        raise ValueError(f"Unsupported cloud storage protocol: {path.scheme}")


def manage_spark_session(spark_name: str) -> SparkSession:
    """
    Manage SparkSession resources.
    :param spark_name: Name of the SparkSession.
    :return: SparkSession object.
    """
    spark = SparkSession.builder.appName(spark_name).getOrCreate()
    try:
        return spark
    finally:
        spark.stop()  # Stop the SparkSession when done


def main():
    with manage_spark_session("Reading Different Data Sources") as spark:
        # Reading CSV data from the local file system
        local_csv_path = pathlib.Path("path/to/csv_file.csv")
        if not local_csv_path.exists():
            raise FileNotFoundError(f"Local CSV file '{local_csv_path}' not found.")

        local_csv_data = read_local_data(spark, "csv", local_csv_path)
        local_csv_data.show()

        # Reading ORC data from the local file system
        local_orc_path = pathlib.Path("path/to/orc_file.orc")
        if not local_orc_path.exists():
            raise FileNotFoundError(f"Local ORC file '{local_orc_path}' not found.")

        local_orc_data = read_local_data(spark, "orc", local_orc_path)
        local_orc_data.show()

        # Reading JSON data from Azure Blob Storage
        azure_json_path = pathlib.Path(
            "wasbs://your-container@your-storage-account.blob.core.windows.net/path/to/json_file.json"
        )
        if not azure_json_path.exists():
            raise FileNotFoundError(f"Azure JSON file '{azure_json_path}' not found.")

        azure_json_data = read_cloud_data(spark, "json", azure_json_path)
        azure_json_data.show()

        # Reading Parquet data from Google Cloud Storage
        gcs_parquet_path = pathlib.Path("gs://your-bucket/path/to/parquet_file.parquet")
        if not gcs_parquet_path.exists():
            raise FileNotFoundError(f"GCS Parquet file '{gcs_parquet_path}' not found.")

        gcs_parquet_data = read_cloud_data(spark, "parquet", gcs_parquet_path)
        gcs_parquet_data.show()

        # Reading data from a directory in Google Cloud Storage
        gcs_directory_path = pathlib.Path(
            "gs://your-bucket/path/to/directory/with/parquet_files"
        )
        if not gcs_directory_path.is_dir():
            raise FileNotFoundError(f"GCS Directory '{gcs_directory_path}' not found.")

        gcs_directory_data = read_cloud_data(spark, "parquet", gcs_directory_path)
        gcs_directory_data.show()

        # Reading data with options from S3
        s3_csv_options = {"header": "true", "inferSchema": "true"}
        s3_csv_with_options_path = pathlib.Path(
            "s3a://your-bucket/path/to/csv_file_with_header.csv"
        )
        if not s3_csv_with_options_path.exists():
            raise FileNotFoundError(
                f"S3 CSV file '{s3_csv_with_options_path}' not found."
            )

        s3_csv_with_options_data = read_cloud_data(
            spark, "csv", s3_csv_with_options_path, options=s3_csv_options
        )
        s3_csv_with_options_data.show()

        # Reading Avro data from Azure Blob Storage
        azure_avro_path = pathlib.Path(
            "wasbs://your-container@your-storage-account.blob.core.windows.net/path/to/avro_file.avro"
        )
        if not azure_avro_path.exists():
            raise FileNotFoundError(f"Azure Avro file '{azure_avro_path}' not found.")

        azure_avro_data = read_cloud_data(spark, "avro", azure_avro_path)
        azure_avro_data.show()


if __name__ == "__main__":  

    cloud_env_setup_main()
    print("Storage is set up. Continuing with the rest of the code...")

    spark = manage_spark_session("Reading Different Data Sources")
    # Reading CSV data from the local file system
    csv_options = {"header": "true", "inferSchema": "true"}
    local_csv_path = pathlib.Path("pyspark-data-ingest\dataset\customers-100.csv")
    if not local_csv_path.exists():
        raise FileNotFoundError(f"Local CSV file '{local_csv_path}' not found.")

    local_csv_data = read_local_data(spark, "csv", local_csv_path, options=csv_options)
    local_csv_data.show()

    # Reading data with options from S3
    s3_csv_options = {"header": "true", "inferSchema": "true"}
    s3_csv_with_options_path = pathlib.Path(
        "s3a://your-bucket/path/to/csv_file_with_header.csv"
    )
    if not s3_csv_with_options_path.exists():
        raise FileNotFoundError(f"S3 CSV file '{s3_csv_with_options_path}' not found.")

    s3_csv_with_options_data = read_cloud_data(
        spark, "csv", s3_csv_with_options_path, options=s3_csv_options
    )
    s3_csv_with_options_data.show()