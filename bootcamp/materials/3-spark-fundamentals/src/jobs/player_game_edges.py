from pyspark.sql import SparkSession




def do_player_game_edges(spark, dataframe):
    
    query =\
"""
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY player_id) AS row_num
    FROM game_details
)
SELECT
    player_id AS subject_identifier,
    'player' as subject_type,
    game_id AS object_identifier,
    'game' AS object_type,
    'plays_in' AS edge_type,
    struct(
        start_position,
        pts,
        team_id,
        team_abbreviation
        ) as properties
FROM deduped
WHERE row_num = 1;
"""
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)



def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_edges") \
      .getOrCreate()
    output_df = do_player_game_edges(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("player_game_edges")