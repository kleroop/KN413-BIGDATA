import pyspark
from pyspark.sql import Window, WindowSpec
import pyspark.sql.types as t
from pyspark.sql.functions import col, dense_rank, split, explode

from workers.BaseTSV import BaseTSV
from models import *


class TitleBasicsWorker(BaseTSV):
    def __init__(self,
                 spark_session: 'pyspark.sql.SparkSession',
                 path: str
                 ):
        super().__init__(spark_session, path, title_basics_schema)

    def get_adult_content_aired_in_region(self,
                                          title_akas_data: BaseTSV,
                                          region: str,
                                          limit: int = 10
                                          ):
        
        return (
            title_akas_data.tsv_df
                .filter(condition=(
                    col(TitleAkasModel.region) == region
                ))
                .select([
                    col(TitleAkasModel.titleId)
                        .alias(TitleBasicsModel.tconst),
                    TitleAkasModel.title
                ])
                .join(
                    other=self.tsv_df
                        .filter(condition=(
                            col(TitleBasicsModel.isAdult).cast(t.BooleanType()) == True
                        ))
                        .select([
                            TitleBasicsModel.tconst,
                            TitleBasicsModel.primaryTitle
                        ]),
                    on=TitleBasicsModel.tconst,
                    how='inner'
                )
                .dropDuplicates([
                    TitleBasicsModel.tconst
                ])
                .limit(num=limit)
        )
    
    def get_titles_of_each_type_ranked_by_runtime(self): 
        runtime_rank_column = 'runtimeRank'

        window_spec: WindowSpec = (
            Window
                .partitionBy(
                    TitleBasicsModel.titleType
                )
                .orderBy(
                    col(TitleBasicsModel.runtimeMinutes)
                        .desc()
                )
        )

        return (
            self.tsv_df
                .filter(condition=(
                    (col(TitleBasicsModel.primaryTitle).isNotNull()) &
                    (col(TitleBasicsModel.runtimeMinutes).isNotNull())
                ))
                .select([
                    TitleBasicsModel.tconst,
                    TitleBasicsModel.titleType,
                    TitleBasicsModel.primaryTitle,
                    TitleBasicsModel.runtimeMinutes
                ])
                .withColumn(runtime_rank_column, dense_rank().over(window_spec))
        )
    
    def get_titles_of_each_genre_ranked_by_most_reviewed(self,
                                                         title_ratings_data: BaseTSV
                                                         ):
        genre_array_column = 'genre_arr'
        genre_separated_column = 'genre_single'
        votes_rank_column = 'votesRank'

        window_spec: WindowSpec = (
            Window
                .partitionBy(
                    genre_separated_column
                )
                .orderBy(
                    col(TitleRatingsModel.numberVotes)
                        .desc()
                )
        )

        return (
            self.tsv_df
                .withColumn(genre_array_column, split(col(TitleBasicsModel.genres), ',').cast(t.ArrayType(t.StringType())))
                .select('*', explode(col(genre_array_column)).alias(genre_separated_column))
                .drop(genre_array_column)
                .join(
                    other=title_ratings_data.tsv_df,
                    on=TitleBasicsModel.tconst,
                    how='inner'
                )
                .withColumn(votes_rank_column, dense_rank().over(window_spec))
                .select([
                    genre_separated_column,
                    TitleBasicsModel.tconst,
                    TitleBasicsModel.primaryTitle,
                    TitleRatingsModel.numberVotes,
                    votes_rank_column
                ])
        )
