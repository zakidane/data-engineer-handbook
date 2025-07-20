# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# %%
spark = SparkSession.builder.appName("zaki-homework3").getOrCreate()

# %%
spark

# %%
#Disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# %%
#Task 2 - pull in spark dataframes, broadcast medals and maps, and join
medals = spark.read.option("header","true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header","true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header","true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header","true").csv("/home/iceberg/data/match_details.csv")
medal_matches_players = spark.read.option("header","true").csv("/home/iceberg/data/medals_matches_players.csv")

# %%
medals_broadcast = broadcast(medals)
maps_broadcast = broadcast(maps)

# %%
joined_maps_medals = medals_broadcast.join(medal_matches_players, on='medal_id', how='inner') \
                                    .join(matches, on='match_id', how='inner') \
                                    .join(maps_broadcast, on='mapid', how='inner')

# %%
# Task 3 - enable config, and bucket join match_details, matches and medal_matches_players on match_id with 16 buckets
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') 
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')




# %%
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bootcamp")
spark.sql("USE demo.bootcamp")

# %%
spark.sql("DROP TABLE IF EXISTS demo.bootcamp.bucketed_matches")
spark.sql("DROP TABLE IF EXISTS demo.bootcamp.bucketed_match_details")
spark.sql("DROP TABLE IF EXISTS demo.bootcamp.bucketed_medal_matches_players")

# %%
matches.write\
    .bucketBy(16,'match_id')\
        .saveAsTable('demo.bootcamp.bucketed_matches')

# %%
match_details.write\
    .bucketBy(16,'match_id')\
        .saveAsTable('demo.bootcamp.bucketed_match_details')

#drop player_gamertag column 
cols_to_keep = [col for col in medal_matches_players.columns if col != 'player_gamertag']
clean_medal_matches_players = medal_matches_players[cols_to_keep]

clean_medal_matches_players.write\
    .bucketBy(16,'match_id')\
        .saveAsTable('demo.bootcamp.bucketed_medal_matches_players')



# %%
bucketed_matches = spark.table('demo.bootcamp.bucketed_matches')
bucketed_match_details = spark.table('demo.bootcamp.bucketed_match_details')
bucketed_medal_matches_players = spark.table('demo.bootcamp.bucketed_medal_matches_players')


# %%
joined_match = bucketed_medal_matches_players.join(bucketed_match_details, on='match_id', how='inner') \
                                    .join(bucketed_matches, on='match_id', how='inner')

# %%
## TASK 4 - Imported necessary aggregation functions and and answers to questions

from pyspark.sql.functions import col, max, avg,countDistinct,count

# %%
# Which player averages the most kills per game?

max_kills_per_match = joined_match.groupBy('player_gamertag','match_id').agg(
    max('player_total_kills').alias('max_total_kills'), 
)

player_avg_kills = max_kills_per_match.groupBy('player_gamertag').agg(
    avg('max_total_kills').alias('avg_total_kills')
)

player_avg_kills.orderBy('avg_total_kills', ascending=False).limit(1).show()

#So gimpinator14 has the highest avg kills per game



# %%
#we check here if one match id might have multiple playlist id's. Result is each match id corresponds only to one playlist id
joined_match.groupBy('match_id') \
    .agg(countDistinct('playlist_id').alias('distinct_playlist_count')) \
    .filter('distinct_playlist_count > 1') \
    .show(truncate=False)


# %%
#aggregate and find the most common playlist here
joined_match.groupBy('playlist_id').agg(
        countDistinct('match_id').alias('count_matches'))\
        .orderBy('count_matches',ascending=False).limit(1).show(truncate=False)
#So playlist id |f72e0ef0-7c4a-4307-af78-8e38dac3fdba with 7640 listens is the most common

# %%
#aggregate and find the most common map
joined_match.groupBy('mapid').agg(
        countDistinct('match_id').alias('count_matches'))\
        .orderBy('count_matches',ascending=False).limit(1).show(truncate=False)

# %%
#Which map do players get the most killing spree medals on? First we grab the medal_id, find the map_id to answer the question and query the map name
medal_for_killing_spree = medals.filter(col('name').contains('Killing Spree')).select('medal_id').distinct().head()[0] # this gets the medalid for killing spree

# %%
max_map_id = joined_maps_medals.filter(col('medal_id') == medal_for_killing_spree)\
                    .select('mapid','medal_id','player_gamertag').distinct()\
                    .groupBy('mapid')\
                    .agg(count('*').alias('medal_count'))\
                    .orderBy('medal_count', ascending=False)\
                    .limit(1)\
                    .head()[0]
maps.filter(col('mapid') == max_map_id).select('name').show() #The result is Breakout Arena here

# %%
#comparing different parquet sizes for sorted within partition df's
#sorting with partition on high cardinality and trying to save to parquet causes out of memory error

sorted_with_map_id = joined_match.sortWithinPartitions("mapid","playlist_id")



# %%



