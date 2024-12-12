WITH with_previous AS (
    SELECT 
        player_name, 
        scoring_class,
        current_season,
        is_active,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
    WHERE current_season <= 2021
),
with_indicators AS (
    SELECT *,
        CASE
            WHEN scoring_class <> previous_scoring_class THEN 1 
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
SELECT 
    player_name, 
    scoring_class,
    is_active,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season
FROM with_streaks
GROUP BY 
    player_name, 
    scoring_class, 
    is_active, 
    streak_identifier
ORDER BY player_name;

WITH with_previous AS (
    SELECT 
        player_name, 
        scoring_class,
        current_season,
        is_active,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
    WHERE current_season <= 2021
),
with_indicators AS (
    SELECT *,
        CASE
            WHEN scoring_class <> previous_scoring_class THEN 1 
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
SELECT 
    player_name, 
    scoring_class,
    is_active,
    MIN(current_season) AS start_season,
    MAX(current_season) AS end_season
FROM with_streaks
GROUP BY 
    player_name, 
    scoring_class, 
    is_active, 
    streak_identifier;

    WITH agg AS (
    SELECT 
        metric_name, 
        month_start, 
        ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
SELECT 
    metric_name, 
    month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL) AS date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);

WITH agg AS (
    SELECT 
        metric_name, 
        month_start, 
        ARRAY(SUM(metric_array[0]), SUM(metric_array[1]), SUM(metric_array[2])) AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
SELECT 
    metric_name, 
    date_add(month_start, index - 1) AS date,
    elem AS value
FROM agg
LATERAL VIEW POSEXPLODE(summed_array) AS index, elem;

from pyspark.sql import SparkSession

def run_query_1(input_path, output_path):
    spark = SparkSession.builder.appName("PlayerScoringClassChanges").getOrCreate()

    # Load Data
    players_df = spark.read.csv(input_path, header=True, inferSchema=True)
    players_df.createOrReplaceTempView("players")

    # Execute SparkSQL Query
    query = """
    WITH with_previous AS (
        SELECT 
            player_name, 
            scoring_class,
            current_season,
            is_active,
            LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
            LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
        FROM players
        WHERE current_season <= 2021
    ),
    with_indicators AS (
        SELECT *,
            CASE
                WHEN scoring_class <> previous_scoring_class THEN 1 
                WHEN is_active <> previous_is_active THEN 1
                ELSE 0
            END AS change_indicator
        FROM with_previous
    ),
    with_streaks AS (
        SELECT *,
            SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
        FROM with_indicators
    )
    SELECT 
        player_name, 
        scoring_class,
        is_active,
        MIN(current_season) AS start_season,
        MAX(current_season) AS end_season
    FROM with_streaks
    GROUP BY 
        player_name, 
        scoring_class, 
        is_active, 
        streak_identifier
    """
    result_df = spark.sql(query)

    # Write Output
    result_df.write.csv(output_path, header=True)

    from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode

def run_query_2(input_path, output_path):
    spark = SparkSession.builder.appName("ArrayMetricsAggregation").getOrCreate()

    # Load Data
    metrics_df = spark.read.csv(input_path, header=True, inferSchema=True)
    metrics_df.createOrReplaceTempView("array_metrics")

    # Execute SparkSQL Query
    query = """
    WITH agg AS (
        SELECT 
            metric_name, 
            month_start, 
            ARRAY(SUM(metric_array[0]), SUM(metric_array[1]), SUM(metric_array[2])) AS summed_array
        FROM array_metrics
        GROUP BY metric_name, month_start
    )
    SELECT 
        metric_name, 
        date_add(month_start, index - 1) AS date,
        elem AS value
    FROM agg
    LATERAL VIEW POSEXPLODE(summed_array) AS index, elem
    """
    result_df = spark.sql(query)

    # Write Output
    result_df.write.csv(output_path, header=True)

    import pytest
from pyspark.sql import SparkSession
from src.jobs.query_1_job import run_query_1

def test_query_1_job():
    spark = SparkSession.builder.master("local").appName("TestQuery1").getOrCreate()
    input_data = [("PlayerA", "Class1", 1, True, 2020),
                  ("PlayerA", "Class2", 1, True, 2021)]
    input_df = spark.createDataFrame(input_data, ["player_name", "scoring_class", "current_season", "is_active"])

    # Mock Paths
    input_path = "/tmp/input.csv"
    output_path = "/tmp/output.csv"

    # Run Job
    run_query_1(input_path, output_path)

    # Assertions
    output_df = spark.read.csv(output_path, header=True)
    assert output_df.count() > 0
import pytest
from pyspark.sql import SparkSession
from src.jobs.query_2_job import run_query_2

def test_query_2_job():
    spark = SparkSession.builder.master("local").appName("TestQuery2").getOrCreate()
    input_data = [(1, "2023-01-01", [10, 20, 30])]
    input_df = spark.createDataFrame(input_data, ["user_id", "month_start", "metric_array"])

    # Mock Paths
    input_path = "/tmp/input.csv"
    output_path = "/tmp/output.csv"

    # Run Job
    run_query_2(input_path, output_path)

    # Assertions
    output_df = spark.read.csv(output_path, header=True)
    assert output_df.count() > 0
