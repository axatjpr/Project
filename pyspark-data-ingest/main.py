from __future__ import absolute_import

from pyspark_data_ingest.src.setup_storage.cloud_env_setup import main as cloud_env_setup_main
from pyspark_data_ingest.src.data_reader.spark_data_reader import read_local_data, read_cloud_data
import utils

if __name__ == "__main__":
    cloud_env_setup_main()
    print("Storage is set up. Continuing with the rest of the code...")

    spark = utils.manage_spark_session("Reading Different Data Sources")

    # Reading CSV data from the local file system
    csv_options = {"header": "true", "inferSchema": "true"}
    local_csv_path = pathlib.Path("pyspark-data-ingest/dataset/customers-100.csv")
    if not local_csv_path.exists():
        raise FileNotFoundError(f"Local CSV file '{local_csv_path}' not found.")

    local_csv_data = read_local_data(spark, "csv", local_csv_path, options=csv_options)
    local_csv_data.show()

    # Reading data with options from S3
    s3_csv_options = {"header": "true", "inferSchema": "true"}
    s3_csv_with_options_path = pathlib.Path("s3a://your-bucket/path/to/csv_file_with_header.csv")
    if not s3_csv_with_options_path.exists():
        raise FileNotFoundError(f"S3 CSV file '{s3_csv_with_options_path}' not found.")

    s3_csv_with_options_data = read_cloud_data(
        spark, "csv", s3_csv_with_options_path, options=s3_csv_options
    )
    s3_csv_with_options_data.show()