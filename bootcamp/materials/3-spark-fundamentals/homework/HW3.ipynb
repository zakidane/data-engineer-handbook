{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "1d19d67d-268d-46bc-bc82-59016c5122e2",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import broadcast"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "75f91e19-9edb-4ecd-86da-ead6f195c7c1",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/07/15 00:27:31 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.\n"
     ]
    }
   ],
   "source": [
    "spark = SparkSession.builder.appName(\"zaki-homework3\").getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "d241f503-6c14-478a-9a82-c2a554a85845",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "\n",
       "            <div>\n",
       "                <p><b>SparkSession - in-memory</b></p>\n",
       "                \n",
       "        <div>\n",
       "            <p><b>SparkContext</b></p>\n",
       "\n",
       "            <p><a href=\"http://09c0e049dfbf:4040\">Spark UI</a></p>\n",
       "\n",
       "            <dl>\n",
       "              <dt>Version</dt>\n",
       "                <dd><code>v3.5.5</code></dd>\n",
       "              <dt>Master</dt>\n",
       "                <dd><code>local[*]</code></dd>\n",
       "              <dt>AppName</dt>\n",
       "                <dd><code>PySparkShell</code></dd>\n",
       "            </dl>\n",
       "        </div>\n",
       "        \n",
       "            </div>\n",
       "        "
      ],
      "text/plain": [
       "<pyspark.sql.session.SparkSession at 0x7fc91fc25810>"
      ]
     },
     "execution_count": 3,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "99b28413-0096-42ea-8761-7e1c191db6b6",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Disable automatic broadcast join\n",
    "spark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", \"-1\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "074a11dc-71f3-4463-8892-43c80a5c5eab",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Task 2 - pull in spark dataframes, broadcast medals and maps, and join\n",
    "medals = spark.read.option(\"header\",\"true\").csv(\"/home/iceberg/data/medals.csv\")\n",
    "maps = spark.read.option(\"header\",\"true\").csv(\"/home/iceberg/data/maps.csv\")\n",
    "matches = spark.read.option(\"header\",\"true\").csv(\"/home/iceberg/data/matches.csv\")\n",
    "match_details = spark.read.option(\"header\",\"true\").csv(\"/home/iceberg/data/match_details.csv\")\n",
    "medal_matches_players = spark.read.option(\"header\",\"true\").csv(\"/home/iceberg/data/medals_matches_players.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "id": "9c81102e-124e-46c0-8be1-350950fdefc9",
   "metadata": {},
   "outputs": [],
   "source": [
    "medals_broadcast = broadcast(medals)\n",
    "maps_broadcast = broadcast(maps)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "c51188a5-2e95-4a76-bfb7-bf0bf4f89fdf",
   "metadata": {},
   "outputs": [],
   "source": [
    "joined_maps_medals = medals_broadcast.join(medal_matches_players, on='medal_id', how='inner') \\\n",
    "                                    .join(matches, on='match_id', how='inner') \\\n",
    "                                    .join(maps_broadcast, on='mapid', how='inner')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "id": "8373e47f-c994-4b16-b9e7-83959cbf06a6",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Task 3 - enable config, and bucket join match_details, matches and medal_matches_players on match_id with 16 buckets\n",
    "spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') \n",
    "spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "f817d064-9794-4c1a-b934-21d37cc81f72",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DataFrame[]"
      ]
     },
     "execution_count": 31,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"CREATE NAMESPACE IF NOT EXISTS demo.bootcamp\")\n",
    "spark.sql(\"USE demo.bootcamp\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "id": "b910e96b-8ebf-489c-a2ac-77b72cf79d42",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DataFrame[]"
      ]
     },
     "execution_count": 32,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark.sql(\"DROP TABLE IF EXISTS demo.bootcamp.bucketed_matches\")\n",
    "spark.sql(\"DROP TABLE IF EXISTS demo.bootcamp.bucketed_match_details\")\n",
    "spark.sql(\"DROP TABLE IF EXISTS demo.bootcamp.bucketed_medal_matches_players\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "0d06b94e-047e-4ed8-9c1e-283422c7577c",
   "metadata": {},
   "outputs": [],
   "source": [
    "matches.write\\\n",
    "    .bucketBy(16,'match_id')\\\n",
    "        .saveAsTable('demo.bootcamp.bucketed_matches')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "2b33fd5e-62b7-4a04-93b7-1b52b9e84f59",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                "
     ]
    }
   ],
   "source": [
    "match_details.write\\\n",
    "    .bucketBy(16,'match_id')\\\n",
    "        .saveAsTable('demo.bootcamp.bucketed_match_details')\n",
    "\n",
    "#drop player_gamertag column \n",
    "cols_to_keep = [col for col in medal_matches_players.columns if col != 'player_gamertag']\n",
    "clean_medal_matches_players = medal_matches_players[cols_to_keep]\n",
    "\n",
    "clean_medal_matches_players.write\\\n",
    "    .bucketBy(16,'match_id')\\\n",
    "        .saveAsTable('demo.bootcamp.bucketed_medal_matches_players')\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "28df43ec-d58f-451b-b2f2-762580b67883",
   "metadata": {},
   "outputs": [],
   "source": [
    "bucketed_matches = spark.table('demo.bootcamp.bucketed_matches')\n",
    "bucketed_match_details = spark.table('demo.bootcamp.bucketed_match_details')\n",
    "bucketed_medal_matches_players = spark.table('demo.bootcamp.bucketed_medal_matches_players')\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "ab998418-19d5-43da-abbf-3613d07f9a3c",
   "metadata": {},
   "outputs": [],
   "source": [
    "joined_match = bucketed_medal_matches_players.join(bucketed_match_details, on='match_id', how='inner') \\\n",
    "                                    .join(bucketed_matches, on='match_id', how='inner')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "id": "4c88aefb-71bc-49ea-85dc-67c78436a0dc",
   "metadata": {},
   "outputs": [],
   "source": [
    "## TASK 4 - Imported necessary aggregation functions and and answers to questions\n",
    "\n",
    "from pyspark.sql.functions import col, max, avg,countDistinct,count"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "id": "221a6d2f-5ed2-47df-a969-96c42cdccc1c",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "25/07/15 00:35:44 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.\n",
      "25/07/15 00:35:44 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.\n",
      "25/07/15 00:35:44 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.\n",
      "25/07/15 00:35:44 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.\n",
      "25/07/15 00:35:45 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.\n",
      "25/07/15 00:35:45 WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.\n",
      "[Stage 59:==========================================>             (12 + 4) / 16]"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+---------------+---------------+\n",
      "|player_gamertag|avg_total_kills|\n",
      "+---------------+---------------+\n",
      "|   gimpinator14|          109.0|\n",
      "+---------------+---------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                "
     ]
    }
   ],
   "source": [
    "# Which player averages the most kills per game?\n",
    "\n",
    "max_kills_per_match = joined_match.groupBy('player_gamertag','match_id').agg(\n",
    "    max('player_total_kills').alias('max_total_kills'), \n",
    ")\n",
    "\n",
    "player_avg_kills = max_kills_per_match.groupBy('player_gamertag').agg(\n",
    "    avg('max_total_kills').alias('avg_total_kills')\n",
    ")\n",
    "\n",
    "player_avg_kills.orderBy('avg_total_kills', ascending=False).limit(1).show()\n",
    "\n",
    "#So gimpinator14 has the highest avg kills per game\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "id": "18b8370e-14e8-42f4-9a54-e7a29d1becb4",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------+-----------------------+\n",
      "|match_id|distinct_playlist_count|\n",
      "+--------+-----------------------+\n",
      "+--------+-----------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#we check here if one match id might have multiple playlist id's. Result is each match id corresponds only to one playlist id\n",
    "joined_match.groupBy('match_id') \\\n",
    "    .agg(countDistinct('playlist_id').alias('distinct_playlist_count')) \\\n",
    "    .filter('distinct_playlist_count > 1') \\\n",
    "    .show(truncate=False)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "id": "e16c5d46-d5c8-41d7-80c9-293a630f38c9",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 65:==========================================>             (12 + 4) / 16]"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------------------------------+-------------+\n",
      "|playlist_id                         |count_matches|\n",
      "+------------------------------------+-------------+\n",
      "|f72e0ef0-7c4a-4307-af78-8e38dac3fdba|7640         |\n",
      "+------------------------------------+-------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                "
     ]
    }
   ],
   "source": [
    "#aggregate and find the most common playlist here\n",
    "joined_match.groupBy('playlist_id').agg(\n",
    "        countDistinct('match_id').alias('count_matches'))\\\n",
    "        .orderBy('count_matches',ascending=False).limit(1).show(truncate=False)\n",
    "#So playlist id |f72e0ef0-7c4a-4307-af78-8e38dac3fdba with 7640 listens is the most common"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "id": "bf8e015d-7264-4fd9-9b3e-af78a53e6d6b",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 68:==========================================>             (12 + 4) / 16]"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------------------------------+-------------+\n",
      "|mapid                               |count_matches|\n",
      "+------------------------------------+-------------+\n",
      "|c7edbf0f-f206-11e4-aa52-24be05e24f7e|7032         |\n",
      "+------------------------------------+-------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                "
     ]
    }
   ],
   "source": [
    "#aggregate and find the most common map\n",
    "joined_match.groupBy('mapid').agg(\n",
    "        countDistinct('match_id').alias('count_matches'))\\\n",
    "        .orderBy('count_matches',ascending=False).limit(1).show(truncate=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "id": "7e587625-d3b1-49f9-9951-11de8adab226",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Which map do players get the most killing spree medals on? First we grab the medal_id, find the map_id to answer the question and query the map name\n",
    "medal_for_killing_spree = medals.filter(col('name').contains('Killing Spree')).select('medal_id').distinct().head()[0] # this gets the medalid for killing spree"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "id": "8f426d70-792a-4cc1-a81e-d59264fe3955",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------+\n",
      "|          name|\n",
      "+--------------+\n",
      "|Breakout Arena|\n",
      "+--------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "max_map_id = joined_maps_medals.filter(col('medal_id') == medal_for_killing_spree)\\\n",
    "                    .select('mapid','medal_id','player_gamertag').distinct()\\\n",
    "                    .groupBy('mapid')\\\n",
    "                    .agg(count('*').alias('medal_count'))\\\n",
    "                    .orderBy('medal_count', ascending=False)\\\n",
    "                    .limit(1)\\\n",
    "                    .head()[0]\n",
    "maps.filter(col('mapid') == max_map_id).select('name').show() #The result is Breakout Arena here"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "id": "74921a02-9333-4b99-8aa4-7e49c92a55ff",
   "metadata": {},
   "outputs": [],
   "source": [
    "#comparing different parquet sizes for sorted within partition df's\n",
    "#sorting with partition on high cardinality and trying to save to parquet causes out of memory error\n",
    "\n",
    "sorted_with_map_id = joined_match.sortWithinPartitions(\"mapid\",\"playlist_id\")\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e38f495b-157a-43da-9a60-bd30a308cc4e",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.16"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
