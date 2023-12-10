from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

from workers import *

spark_session = (SparkSession.builder
                 .master('local')
                 .appName('test app')
                 .config(conf=SparkConf())
                 .getOrCreate())

name_basics = NameBasicsWorker(spark_session, "data/name.basics.tsv")
titles_akas = TitleAkasWorker(spark_session, "data/title.akas.tsv")
titles_basics = TitleBasicsWorker(spark_session, "data/title.basics.tsv")
titles_crew = TitleCrewWorker(spark_session, "data/title.crew.tsv")
titles_episode = TitleEpisodeWorker(spark_session, "data/title.episode.tsv")
titles_principals = TitlePrincipalsWorker(spark_session, "data/title.principals.tsv")
titles_ratings = TitleRatingsWorker(spark_session, "data/title.ratings.tsv")

print('get_adult_content_aired_in_region')
titles_basics.get_adult_content_aired_in_region(title_akas_data=titles_akas, region="UA", limit=20).show()
print('get_titles_with_longest_runtime')
titles_basics.get_titles_with_longest_runtime(limit=25).show()
print('get_titles_sorted_by_most_regions_aired')
titles_akas.get_titles_sorted_by_most_regions_aired(title_basics_data=titles_basics, limit=20).show()
print('get_series_sorted_by_episodes_number')
titles_episode.get_series_sorted_by_episodes_number(title_basics_data=titles_basics, limit=20).show()
print('get_titles_sorted_by_composers_number')
titles_principals.get_titles_sorted_by_composers_number(title_basics_data=titles_basics, limit=20).show()
print('get_titles_sorted_by_votes_number')
titles_ratings.get_titles_sorted_by_votes_number(title_basics_data=titles_basics, limit=20).show()
