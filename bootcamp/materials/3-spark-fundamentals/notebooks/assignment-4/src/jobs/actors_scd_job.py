from pyspark.sql import SparkSession


query = """
    WITH with_previous AS (
                SELECT * 
                ,
                    LAG(quality_class, 1) OVER(PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
                    LAG(is_active, 1) OVER(PARTITION BY actor ORDER BY current_year) AS previous_is_active
                FROM actors
                WHERE current_year <=2021
        ), 
        with_indicators AS (
                SELECT *
                    ,CASE
                        WHEN is_active <> previous_is_active THEN 1
                        WHEN quality_class <> previous_quality_class THEN 1
                        ELSE 0
                    END AS change_indicator
                FROM with_previous
        ),
        with_streaks AS (
                SELECT *,
                    SUM(change_indicator) OVER (PARTITION BY actor order by current_year) as streak_identifier
                FROM with_indicators
        )
    SELECT
        actor
        ,2021 as current_year
        ,films
        ,quality_class
        ,is_active
        ,MIN(current_year) AS start_year
        ,MAX(current_year) AS end_year
    FROM with_streaks
    GROUP BY actor, quality_class,is_active,films
    ORDER BY actor, current_year
"""


def do_actor_scd_transformation(spark, dataframe):
    '''used for crateing the teamp table based on the given dataset'''

    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    '''entry point of the program '''
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")
