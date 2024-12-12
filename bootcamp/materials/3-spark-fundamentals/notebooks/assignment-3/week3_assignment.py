from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, broadcast

# Initialize Spark Session
def initialize_spark(app_name: str) -> SparkSession:
    """Initialize Spark Session with required configuration."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .enableHiveSupport() \
        .getOrCreate()

# Load data from CSV
def load_data(spark: SparkSession, path: str):
    """Load CSV data into a DataFrame."""
    return spark.read.csv(path, header=True, inferSchema=True)

# Bucket data
def bucket_data(df, bucket_column: str, num_buckets: int):
    """Repartition DataFrame into buckets."""
    return df.repartitionByRange(num_buckets, bucket_column)

# Perform bucketed join
def bucketed_join(df_left, df_right, join_column: str, join_type: str = "inner"):
    """Perform a bucket join."""
    return df_left.join(df_right, join_column, join_type)

# Perform broadcast join
def join_with_broadcast(df_left, df_right, join_column: str, join_type: str = "inner"):
    """Perform a broadcast join."""
    return df_left.join(broadcast(df_right), join_column, join_type)

# Aggregation queries
def calculate_top_player_avg_kills(match_details_df):
    """Calculate which player averages the most kills per game."""
    return match_details_df.groupBy("player_gamertag").agg(
        avg("player_total_kills").alias("avg_kills_per_game")
    ).orderBy(desc("avg_kills_per_game")).limit(1)

def calculate_top_most_played_playlist(matches_df):
    """Determine which playlist gets played the most."""
    return matches_df.groupBy("playlist_id").agg(
        count("match_id").alias("total_matches")
    ).orderBy(desc("total_matches")).limit(1)

def calculate_top_most_played_map(matches_df):
    """Determine which map gets played the most."""
    return matches_df.groupBy("mapid").agg(
        count("match_id").alias("total_matches")
    ).orderBy(desc("total_matches")).limit(1)

def calculate_top_killing_spree_medals(complete_data_with_medals):
    """Determine which map has the most Killing Spree medals."""
    return complete_data_with_medals.filter(
        col("name") == "Killing Spree"
    ).groupBy("mapid").agg(
        count("medal_id").alias("total_killing_spree_medals")
    ).orderBy(desc("total_killing_spree_medals")).limit(1)

# Demonstrate partitioning and sorting
def demonstrate_partitioning_and_sort(df, partition_column: str, num_partitions: int, sort_column: str):
    """Demonstrate partitioning and sorting within partitions."""
    repartitioned_df = df.repartition(num_partitions, col(partition_column))
    sorted_df = repartitioned_df.sortWithinPartitions(sort_column)
    print(f"Partitions: {sorted_df.rdd.getNumPartitions()}")
    sorted_df.explain(True)
    return sorted_df

def main():
    """Main function to execute the Spark job."""
    spark = initialize_spark("Assignment 3 - Functional Programming")

    # Load datasets
    match_details_df = load_data(spark, "/home/iceberg/data/match_details.csv")
    matches_df = load_data(spark, "/home/iceberg/data/matches.csv")
    medals_matches_players_df = load_data(spark, "/home/iceberg/data/medals_matches_players.csv")
    medals_df = load_data(spark, "/home/iceberg/data/medals.csv")

    # Bucket data
    bucketed_match_details = bucket_data(match_details_df, "match_id", 16)
    bucketed_matches = bucket_data(matches_df, "match_id", 16)
    bucketed_medals_matches_players = bucket_data(medals_matches_players_df, "match_id", 16)

    # Perform bucketed joins
    matches_with_details = bucketed_join(bucketed_match_details, bucketed_matches, "match_id")
    complete_data = bucketed_join(matches_with_details, bucketed_medals_matches_players, "match_id")

    # Broadcast join with medals
    complete_data_with_medals = join_with_broadcast(complete_data, medals_df, "medal_id")

    # Aggregations
    top_player_avg_kills = calculate_top_player_avg_kills(match_details_df)
    top_most_played_playlist = calculate_top_most_played_playlist(matches_df)
    top_most_played_map = calculate_top_most_played_map(matches_df)
    top_killing_spree_medals = calculate_top_killing_spree_medals(complete_data_with_medals)

    # Partitioning demonstration
    demonstrate_partitioning_and_sort(match_details_df, "player_gamertag", 8, "player_total_kills")
    demonstrate_partitioning_and_sort(matches_df, "playlist_id", 4, "match_id")

    # Show results
    top_player_avg_kills.show()
    top_most_played_playlist.show()
    top_most_played_map.show()
    top_killing_spree_medals.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()