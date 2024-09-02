import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from code.pipeline.transform import create_dim_player, convert_to_euro


class MyTestCase(unittest.TestCase):

    spark = SparkSession.builder \
             .appName("BettingGamesDataPipelineTest") \
             .getOrCreate()

    def test_create_dim_player(self):
        players = [Row(playerID=1, country="SE", BirthDate="1000", gender="FEMALE", playerState="VALIDATED", vip=0, kyc=0, wantsNewsletter=0, latestUpdate="2017-01-01"),
                   Row(playerID=1, country="SE", BirthDate="1000", gender="FEMALE", playerState="VALIDATED", vip=1, kyc=1, wantsNewsletter=0, latestUpdate="2017-01-02")]

        players_df: DataFrame = self.spark.createDataFrame(players)

        expected = [Row(playerId=1, gender="FEMALE", country="SE", latestUpdate="2017-01-02")]

        expected_df = self.spark.createDataFrame(expected)

        result_df = create_dim_player(players_df)

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_convert_to_euro(self):
        input = [Row(Date="2017-03-20", realAmount=1.0, bonusAmount=0.0, channelUID="DESKTOP", txCurrency="SEK",
                     gameID=1, txType="WAGER", betId=1, playerId=1, currency="SEK", baseRateEuro=0.100),
                 Row(Date="2017-03-21", realAmount=1.0, bonusAmount=0.0, channelUID="DESKTOP", txCurrency="EUR",
                     gameID=1, txType="WAGER", betId=1, playerId=1, currency="SEK", baseRateEuro=1.0)]

        input_df: DataFrame = self.spark.createDataFrame(input)

        expected = [Row(Date="2017-03-20", realAmount=1.0, bonusAmount=0.0, channelUID="DESKTOP", txCurrency="SEK",
                        gameID=1, txType="WAGER", betId=1, playerId=1, currency="SEK", baseRateEuro=0.100,
                        real_amount_euro=0.1, bonus_amount_euro=0.0),
                    Row(Date="2017-03-21", realAmount=1.0, bonusAmount=0.0, channelUID="DESKTOP", txCurrency="EUR",
                        gameID=1, txType="WAGER", betId=1, playerId=1, currency="SEK", baseRateEuro=1.0,
                        real_amount_euro=1.0, bonus_amount_euro=0.0)]

        expected_df = self.spark.createDataFrame(expected)

        result_df = convert_to_euro(input_df)

        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == '__main__':
    unittest.main()
