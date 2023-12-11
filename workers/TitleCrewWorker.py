import pyspark
import pyspark.sql.types as t
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, explode

from workers.BaseTSV import BaseTSV
from models import *


class TitleCrewWorker(BaseTSV):
    def __init__(self,
                 spark_session: 'pyspark.sql.SparkSession',
                 path: str
                 ):
        super().__init__(spark_session, path, title_crew_schema)

    def get_top_directors_by_titles_directed(self,
                                                  name_basics: BaseTSV):
        name_basics_df: DataFrame = name_basics.tsv_df
        exploded_df = self.tsv_df.filter(col("directors").isNotNull())
        # Explode the genres column to have one row per genre

        # Convert the genre column to an array of strings
        exploded_df = exploded_df.withColumn("directors",
                                             split(col("directors"), ",").cast(t.ArrayType(t.StringType())))

        # Explode the array of genres to have one row per genre
        exploded_df = exploded_df.select("*", explode(col("directors")).alias("nconst")).drop("directors")

        exploded_df = exploded_df.groupBy("nconst").count().withColumnRenamed("count", "titlesDirected")

        exploded_df = name_basics_df.join(exploded_df, "nconst", "right")


        exploded_df = exploded_df.select(["primaryName", "titlesDirected"]).orderBy(col("titlesDirected").desc())

        return exploded_df
