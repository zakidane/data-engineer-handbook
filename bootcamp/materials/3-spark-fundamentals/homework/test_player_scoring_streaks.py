from chispa.dataframe_comparer import *
from ..jobs.player_scoring_streaks import do_player_scoring_streaks
from collections import namedtuple


Players = namedtuple("Players", "player_name current_season scoring_class")
Streaks = namedtuple("Streaks", "player_name scoring_class start_date end_date")

def test_do_player_scoring_streaks(spark):
    source_data = [
        Players("Michael Jordan", 2001, 5),
        Players("Michael Jordan", 2003, 5),
        Players("Michael Jordan", 2002, 5),
        Players("Michael Jordan", 2004, 6),
        Players("Michael Jordan", 2005, 6)

    ]

    source_df = spark.createDataFrame(source_data)
    actual_df = do_player_scoring_streaks(spark,source_df)

    expected_data = [
        Streaks("Michael Jordan", 5, 2001, 2003),
        Streaks("Michael Jordan", 6, 2004, 2005)
    ]

    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df)






    

    """
        PlayerSeason = namedtuple("PlayerSeason", "player_name current_season scoring_class")
        PlayerScd = namedtuple("PlayerScd", "player_name scoring_class start_date end_date")


        def test_scd_generation(spark):
            source_data = [
                PlayerSeason("Michael Jordan", 2001, 'Good'),
                PlayerSeason("Michael Jordan", 2002, 'Good'),
                PlayerSeason("Michael Jordan", 2003, 'Bad'),
                PlayerSeason("Someone Else", 2003, 'Bad')
            ]
            source_df = spark.createDataFrame(source_data)

            actual_df = do_player_scd_transformation(spark, source_df)
            expected_data = [
                PlayerScd("Michael Jordan", 'Good', 2001, 2002),
                PlayerScd("Michael Jordan", 'Bad', 2003, 2003),
                PlayerScd("Someone Else", 'Bad', 2003, 2003)
            ]
            expected_df = spark.createDataFrame(expected_data)
            assert_df_equality(actual_df, expected_df)

        """