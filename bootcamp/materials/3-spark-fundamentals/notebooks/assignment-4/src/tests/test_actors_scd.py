'''this test case is used to validate the actor scd'''

from collections import namedtuple
from chispa.dataframe_comparer import assert_df_equality
from jobs.actors_scd_job import do_actor_scd_transformation

ActorSeason = namedtuple("ActorSeason", "Actor current_year quality_class")
ActorScd = namedtuple("ActorScd", "actor quality_class start_date end_date")

def test_scd_generation(spark):
    '''test case for validating the actor scd'''
    source_data = [
        ActorSeason("Alan Alda", 2001, 'Good'),
        ActorSeason("Alan Alda", 2002, 'Good'),
        ActorSeason("Alan Alda", 2003, 'Bad'),
        ActorSeason("Alex Rocco", 2003, 'Bad')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Alan Alda", 'Good', 2001, 2002),
        ActorScd("Alan Alda", 'Bad', 2003, 2003),
        ActorScd("Alex Rocco", 'Bad', 2003, 2003)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
