from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers import *
from models import *

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

print('get_titles_of_each_type_ranked_by_runtime')
each_type_by_runtime_df = titles_basics.get_titles_of_each_type_ranked_by_runtime()
each_type_by_runtime_df.show()
print('get_titles_of_each_type_ranked_by_runtime:type=movie')
each_type_by_runtime_df.filter(col(TitleBasicsModel.titleType) == 'movie').show()

print('get_titles_of_each_genre_ranked_by_most_reviewed')
each_genre_by_most_reviewed_df = titles_basics.get_titles_of_each_genre_ranked_by_most_reviewed(title_ratings_data=titles_ratings)
each_genre_by_most_reviewed_df.show()
print('get_titles_of_each_genre_ranked_by_most_reviewed:genre=History')
each_genre_by_most_reviewed_df.filter(col('genre_single') == 'History').show()

print('get_titles_sorted_by_most_regions_aired')
titles_akas.get_titles_sorted_by_most_regions_aired(title_basics_data=titles_basics, limit=20).show()

print('get_series_sorted_by_episodes_number')
titles_episode.get_series_sorted_by_episodes_number(title_basics_data=titles_basics, limit=20).show()

print('get_titles_sorted_by_composers_number')
titles_principals.get_titles_sorted_by_composers_number(title_basics_data=titles_basics, limit=20).show()
