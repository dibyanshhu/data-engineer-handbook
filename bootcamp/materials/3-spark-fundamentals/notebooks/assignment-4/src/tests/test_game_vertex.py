'''games test case'''

from collections import namedtuple
from chispa.dataframe_comparer import assert_df_equality
from jobs.game_vertex_job import do_game_vertex_transformation


GameVertex = namedtuple("GameVertex", "identifier type properties")
Game = namedtuple("Game", "game_id game_status_text home_team_id visitor_team_id\
                  season home_team_wins")


def test_vertex_generation(spark):
    '''validate the game vertex function'''
    input_data = [
        Game(22200477, "Final", 1610612740, 1610612759, 2022, 1),
        Game(22200477, "Final", 1610612740, 1610612759, 2022, 1),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_game_vertex_transformation(spark, input_dataframe)
    expected_output = [
        GameVertex(
            identifier=1,
            type='game',
            properties={
                'game_status_text': 'Final',
                'home_team_id': 1610612740,
                'visitor_team_id': 1610612759,
                'season': 2022,
                'home_team_wins' :1 
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
