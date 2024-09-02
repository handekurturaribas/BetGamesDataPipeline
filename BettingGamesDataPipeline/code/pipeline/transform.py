from typing import List, Tuple
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, regexp_replace, sum, when, desc, row_number, lit
from pyspark.sql.window import Window


def create_fact_bet(game_transaction_df: DataFrame, currency_exchange_df: DataFrame, player_df: DataFrame) -> DataFrame:
    """
    This method takes Game Transaction, Currency Exchange and Player dataframes as parameters and creates the fact_bet table.
    It calculates cash_turnover, bonus_turnover, cash_winnings, bonus_winnings, turnover, winnings,
    cash_result, bonus_result, gross_result columns in euro currency according to the exchange rate of that day.
    All the calculated columns show values in Euro currency.

    Calculation details:
     * Cash turnover = Wagered amount in cash
     * Bonus turnover = Wagered amount in bonus
     * Cash winnings = Result amount in cash
     * Bonus winnings = Result amount in bonus
     * Turnover = Cash turnover + Bonus turnover
     * Winnings = Cash winnings + Bonus winnings
     * Cash result = Cash turnover - Cash winnings
     * Bonus result = Bonus turnover - Bonus winnings
     * Gross result = Turnover - Winnings

    :param game_transaction_df: Game Transaction csv
    :param currency_exchange_df: Currency Exchange csv
    :param player_df: Player csv
    :return: fact_bet DataFrame
    """
    cols_to_rename = [("Date", "date"), ("playerId", "player_id"), ("gameId", "game_id")]
    # Casting operations, comma is not a delimiter for currency, you need to replace with point instead of comma. After then, we are ready to convert as float with cast.
    game_transaction_df = (game_transaction_df
                           .withColumn('realAmount', regexp_replace('realAmount', ',', '.').cast('float'))
                           .withColumn('bonusAmount', regexp_replace('bonusAmount', ',', '.').cast('float')))

    # join based on date and currency type, because the euro rate is variable for each day.
    game_transaction_with_currency_df: DataFrame = (game_transaction_df
                                                    .join(currency_exchange_df,
                                                          on=(game_transaction_df['Date'] == currency_exchange_df[
                                                              'date']) &
                                                             (game_transaction_df['txCurrency'] == currency_exchange_df[
                                                                 'currency']))
                                                    .drop(currency_exchange_df['date']))
    # Convert to Euro currency - multiply amounts columns with rate euro column, it produces euro based values.
    game_transaction_with_euro_curr_df = convert_to_euro(df=game_transaction_with_currency_df)

    # Add country column from Player dataframe
    game_transaction_with_country_df = game_transaction_with_euro_curr_df.join(player_df.select('playerId', 'country'),
                                                                               on=['playerId'], how='left')

    # If we need to calculate cash turnover. We can sum real amount column where txType is wager etc. TxTypes: Wager & Result
    fact_bet_df = (game_transaction_with_country_df
                   .withColumn("cash_turnover",
                               when(col("txType") == lit("WAGER"), col("real_amount_euro"))
                               .otherwise(0))
                   .withColumn("bonus_turnover",
                               when(col("txType") == lit("WAGER"), col("bonus_amount_euro"))
                               .otherwise(0))
                   .withColumn("cash_winnings",
                               when(col("txType") == lit("RESULT"), col("real_amount_euro"))
                               .otherwise(0))
                   .withColumn("bonus_winnings",
                               when(col("txType") == lit("RESULT"), col("bonus_amount_euro"))
                               .otherwise(0))
                   .groupby('date', 'playerId', 'country', 'gameId')
                   .agg(sum("cash_turnover").alias("cash_turnover"),
                        sum("bonus_turnover").alias("bonus_turnover"),
                        sum("cash_winnings").alias("cash_winnings"),
                        sum("bonus_winnings").alias("bonus_winnings"))
                   .withColumn('turnover', col('cash_turnover') + col('bonus_turnover'))
                   .withColumn('winnings', col('cash_winnings') + col('bonus_winnings'))
                   .withColumn('cash_result', col('cash_turnover') - col('cash_winnings'))
                   .withColumn('bonus_result', col('bonus_turnover') - col('bonus_winnings'))
                   .withColumn('gross_result', col('turnover') - col('winnings'))
                   # .filter(col('playerId') == '16')
                   # .filter(col('gameId') == '2321')
                   )
    # it takes a tuple list containing only the old and new names as parameters. Then, it replaces the 1st element of the tuple with the 0th element.
    final_fact_bet_df = rename_columns(cols_to_rename=cols_to_rename, df=fact_bet_df).orderBy(desc('date'))

    return final_fact_bet_df


def convert_to_euro(df: DataFrame) -> DataFrame:
    """
    This method converts any currency to euro currency with respect to rate at that day

    :param df: Game Transaction DataFrame
    :return: Game Transaction DataFrame with Real Amount & Bonus Amount calculated in Euro currency

    """
    # Calculate Real Amount & Bonus Amount in EUR currency
    # Multiply the relevant day's euro rate by the real and bonus amount columns.
    game_transaction_with_eur_curr_df = (df
                                         .withColumn('real_amount_euro', col('realAmount') * col('baseRateEuro'))
                                         .withColumn('bonus_amount_euro', col('bonusAmount') * col('baseRateEuro'))
                                         )
    return game_transaction_with_eur_curr_df


def create_dim_player(player_df: DataFrame) -> DataFrame:
    """
    This function create dim_player DataFrame having player info per each ID containing only the latest date.

    :param player_df: Player DataFrame
    :return: dim_player
    """
    cols_to_rename = [("playerID", "player_id"), ("latestUpdate", "latest_update")]

    window_spec = Window.partitionBy('playerId').orderBy(desc("latestUpdate"))

    dim_player_df = (player_df
                     .withColumn("rank", row_number().over(window_spec))
                     .filter(col("rank") == lit(1))
                     .drop("rank")
                     .select('playerId', 'gender', 'country', 'latestUpdate'))

    final_dim_player_df = rename_columns(cols_to_rename=cols_to_rename, df=dim_player_df)

    return final_dim_player_df


def create_dim_game(game_df: DataFrame, game_category_df: DataFrame, game_provider_df: DataFrame) -> DataFrame:
    """
    This method uses the Game, Game Category and Game Provider dataframes by performing a left join,
    allowing us to retrieve the game category and provider names from the other dataframes.
    We use a left join to ensure that we can see the unique table based on the game ID, even if the category and
    provider names are empty.

    :param game_df: Game DataFrame
    :param game_category_df: Game Category DataFrame
    :param game_provider_df: Game Provider DataFrame
    :return: dim_game
    """
    cols_to_rename = [("ID", "game_id"), ("Game Name", "game_name"), ("Game Category", "game_category"),
                      ("Game Provider Name", "game_provider_name")]

    game_with_category_df = (
        game_df.join(game_category_df, on=game_df["ID"] == game_category_df["Game ID"], how='left'))

    dim_game_df = (game_with_category_df
                   .join(game_provider_df, on=game_with_category_df["GameProviderId"] == game_provider_df["ID"],
                         how='left')
                   .drop(game_provider_df["ID"])
                   .select('ID', 'Game Name', 'Game Category', 'Game Provider Name'))

    dim_game_df = rename_columns(cols_to_rename=cols_to_rename, df=dim_game_df)

    return dim_game_df


def rename_columns(cols_to_rename: List[Tuple[str, str]], df: DataFrame) -> DataFrame:
    """
    This method changes the names of the columns in the desired dataframe.

    :param cols_to_rename: List of tuples containing both old and new name of the columns
    :param df: Input DataFrame
    :return: Target DataFrame
    """
    for rename_info in cols_to_rename:
        df = df.withColumnRenamed(rename_info[0], rename_info[1])
    return df
