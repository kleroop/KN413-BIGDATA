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

name_basics.tsv_df.show()

titles_akas = TitleAkasWorker(spark_session, "data/title.akas.tsv")

titles_akas.tsv_df.show()

titles_basics = TitleBasicsWorker(spark_session, "data/title.basics.tsv")

titles_basics.tsv_df.show()

titles_crew = TitleCrewWorker(spark_session, "data/title.crew.tsv")

titles_crew.tsv_df.show()

titles_episode = TitleEpisodeWorker(spark_session, "data/title.episode.tsv")

titles_episode.tsv_df.show()

titles_principals = TitlePrincipalsWorker(spark_session, "data/title.principals.tsv")

titles_principals.tsv_df.show()

titles_ratings = TitleRatingsWorker(spark_session, "data/title.ratings.tsv")

titles_ratings.tsv_df.show()