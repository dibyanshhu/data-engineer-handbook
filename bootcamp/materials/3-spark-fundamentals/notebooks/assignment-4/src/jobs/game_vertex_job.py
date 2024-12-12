'''games dedupe'''
from pyspark.sql import SparkSession

# queury used to remove the duplicate records based on the game date
query = """
    WITH games_deduped AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY game_id ORDER BY game_date_est) as row_num
        FROM games
    )
    SELECT
        game_id AS identifier,
        'game' AS `type`,
        map(
            'game_status_text', game_status_text,
            'home_team_id', home_team_id,
            'visitor_team_id', visitor_team_id,
            'season', season,
            'home_team_wins', home_team_wins
            ) AS properties
    FROM games_deduped
    WHERE row_num = 1
"""


def do_game_vertex_transformation(spark, dataframe):
    '''create the temp table and run the sql query'''

    dataframe.createOrReplaceTempView("games")
    return spark.sql(query)


def main():
    '''entry point of this script'''

    spark = SparkSession.builder \
        .master("local") \
        .appName("games") \
        .getOrCreate()
    output_df = do_game_vertex_transformation(spark, spark.table("games"))
    output_df.write.mode("overwrite").insertInto("games_scd")
