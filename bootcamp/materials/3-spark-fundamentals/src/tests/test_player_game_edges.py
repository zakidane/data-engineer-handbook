from chispa.dataframe_comparer import *
#from ..jobs.team_vertex_job import do_team_vertex_transformation
from ..jobs.player_game_edges import do_player_game_edges
from collections import namedtuple
from pyspark.sql import Row

GameDetails = namedtuple("GameDetails","player_id game_id player plays_in start_position pts team_id team_abbreviation")
Edges = namedtuple("Edges", "subject_identifier subject_type object_identifier object_type edge_type properties ")


def test_do_player_game_edges(spark):
    source_data = [
        GameDetails(1, 34, "Michael Jordan", "" "NBA", "Forward", 34, 3, "CB"),
        GameDetails(1, 34, "Michael Jordan", "NBA", "Guard", 34, 3, "CB"),
        GameDetails(1, 36, "Michael Jordan", "NBA", "Forward", 34, 3, "CB")
    ]

    source_df = spark.createDataFrame(source_data)

    actual_df = do_player_game_edges(spark, source_df)

    expected_data = [
        Edges(
                1,
                'player',
                34, 
                'game', 
                'plays_in',
                Row(
                    start_position="Forward",
                    pts=34,
                    team_id=3,
                    team_abbreviation="CB"
                ) 
             ),
        Edges(
                1,
                'player',
                36, 
                'game', 
                'plays_in',
            Row(
                    start_position="Forward",
                    pts=34,
                    team_id=3,
                    team_abbreviation="CB"
            )
             )

    ]

    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df,expected_df,ignore_nullable=True)






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