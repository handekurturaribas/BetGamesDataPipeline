import yaml
from pyspark.sql import SparkSession
from code.pipeline.extract import create_df
from code.pipeline.load import write_to_csv
from code.pipeline.transform import create_fact_bet, create_dim_player, create_dim_game
import logging


# Before running the program, packages need to be installed running 'pip install -r requirements.txt'
# YAML and Requirements: The reason for this is to make the project more modular and dynamic. In this way, maintenance became flexible and easy.

def main():
    try:
        with open("../code/config/config.yaml", 'r') as file:
            config = yaml.safe_load(file)
        logger.info("Config file has been read successfully!")

        # Create dataframe from csv files
        # Dynamically read the csv files we have with the YAML file.
        player_df = create_df(spark=spark, file_path=config['data']['input']['player_source_path'])
        currency_exchange_df = create_df(spark=spark,file_path=config['data']['input']['currency_exchange_source_path'])
        game_df = create_df(spark=spark, file_path=config['data']['input']['game_source_path'])
        game_category_df = create_df(spark=spark, file_path=config['data']['input']['game_category_source_path'])
        game_provider_df = create_df(spark=spark, file_path=config['data']['input']['game_provider_source_path'])
        game_transaction_df = create_df(spark=spark, file_path=config['data']['input']['game_transaction_source_path'],
                                        delimiter=";")

        # Call transform methods
        fact_bet = create_fact_bet(game_transaction_df, currency_exchange_df, player_df)
        dim_player = create_dim_player(player_df)
        dim_game = create_dim_game(game_df, game_category_df, game_provider_df)

        # Write latest dataframes as csv: Fact_Bet, Dim_Player, Dim_Game
        write_info = [(fact_bet, config['data']['output']['fact_bet_output_path']),
                      (dim_player, config['data']['output']['dim_player_output_path']),
                      (dim_game, config['data']['output']['dim_game_output_path'])]

        # fact_bet.show()

        for item in write_info:
            write_to_csv(item[0], item[1])

    except KeyError:
        logger.error("Key not found in the dictionary, please check config.yaml file")
    except FileNotFoundError:
        logging.error("File not found, please check config.yaml file!")
    except yaml.YAMLError as e:
        logging.error(f"Error while reading yaml file!: {e}")
    except Exception as e:
        logging.error(f"Unexpected error {e}")

    # End SparkSession
    spark.stop()


if __name__ == "__main__":
    # Create spark session
    spark = SparkSession.builder \
        .appName("BettingGamesDataPipeline") \
        .getOrCreate()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()
    main()
