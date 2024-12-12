from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, desc, count, sum, avg

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Spark Fundamentals Week") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \  # Disable automatic broadcast joins
    .getOrCreate()

# Load Data
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/path/to/match_details.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/path/to/matches.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/path/to/medals_matches_players.csv")
medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/path/to/medals.csv")

# Create Temporary Views
match_details.createOrReplaceTempView("match_details")
matches.createOrReplaceTempView("matches")
medals_matches_players.createOrReplaceTempView("medals_matches_players")
medals.createOrReplaceTempView("medals")

# Step 1: Explicitly Broadcast Join Medals and Maps
explicit_broadcast_join = medals_matches_players.join(
    broadcast(matches), "match_id"
)

explicit_broadcast_join.show()

# Step 2: Bucket Join on `match_id` with 16 Buckets
# Bucket write for match_details
match_details.write.bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_match_details")

# Bucket write for matches
matches.write.bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_matches")

# Bucket write for medals_matches_players
medals_matches_players.write.bucketBy(16, "match_id").sortBy("match_id").saveAsTable("bucketed_medals_matches_players")

# Perform a bucketed join
bucketed_join = spark.sql("""
SELECT *
FROM bucketed_match_details m1
JOIN bucketed_matches m2 ON m1.match_id = m2.match_id
JOIN bucketed_medals_matches_players m3 ON m1.match_id = m3.match_id
""")

bucketed_join.show()

# Step 3: Aggregation Questions

# 3a: Which player averages the most kills per game?
most_kills_per_game = match_details.groupBy("player_gamertag") \
    .agg((sum("kills") / count("match_id")).alias("avg_kills")) \
    .orderBy(desc("avg_kills")) \
    .limit(1)
most_kills_per_game.show()

# 3b: Which playlist gets played the most?
most_played_playlist = matches.groupBy("playlist_id") \
    .count() \
    .orderBy(desc("count")) \
    .limit(1)
most_played_playlist.show()

# 3c: Which map gets played the most?
most_played_map = matches.groupBy("map_name") \
    .count() \
    .orderBy(desc("count")) \
    .limit(1)
most_played_map.show()

# 3d: Which map do players get the most Killing Spree medals on?
killing_spree_map = medals_matches_players.filter(col("medal_name") == "Killing Spree") \
    .groupBy("map_name") \
    .count() \
    .orderBy(desc("count")) \
    .limit(1)
killing_spree_map.show()

# Step 4: Optimize Data Size Using .sortWithinPartitions
# Partitioning by Playlist and Sorting Within Partitions
partitioned_by_playlist = matches.repartition("playlist_id").sortWithinPartitions("match_id")
partitioned_by_playlist.write.saveAsTable("partitioned_by_playlist")

# Partitioning by Map and Sorting Within Partitions
partitioned_by_map = matches.repartition("map_name").sortWithinPartitions("match_id")
partitioned_by_map.write.saveAsTable("partitioned_by_map")

# Partitioning by Completion Date and Sorting Within Partitions
partitioned_by_date = matches.repartition("completion_date").sortWithinPartitions("match_id")
partitioned_by_date.write.saveAsTable("partitioned_by_date")

# Finalize the session
spark.stop()
