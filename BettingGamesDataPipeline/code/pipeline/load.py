from pyspark.sql import DataFrame


def write_to_csv(df: DataFrame, output_file_path: str) -> None:
    """
    This method writes output dataframes as csv. The target file paths are located in the config file.

    :param df: Output dataframe to be written.
    :param output_file_path: Target file path retrieved from config.yaml
    :return: None
    """
    df.write.mode("overwrite").csv(output_file_path, header=True)
