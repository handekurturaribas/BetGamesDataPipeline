from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def create_df(spark: SparkSession, file_path: str, delimiter: str = ',', header: bool = True,
              infer_schema: bool = True) -> DataFrame:
    """
    This method reads the source csv files from the paths in the config yaml file and creates dataframes

    :param spark: Spark object
    :param file_path: Input data file paths
    :param delimiter: Delimiter to be used for read, default is comma (,)
    :param header: Header to be used for read, default is True
    :param infer_schema: InferSchema option with default value of True
    :return: Input data frames; Game, Game Transaction, Game Category, Game Provider, Player, Currency Exchange DataFrames
    """
    input_df = spark.read.option("delimiter", delimiter).csv(file_path, header=header, inferSchema=infer_schema)
    return input_df
