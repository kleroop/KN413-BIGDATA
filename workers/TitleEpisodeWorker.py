import pyspark
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers.BaseTSV import BaseTSV
from models import *


class TitleEpisodeWorker(BaseTSV):
    def __init__(self,
                 spark_session: 'pyspark.sql.SparkSession',
                 path: str
                 ):
        super().__init__(spark_session, path, title_episode_schema)

    def get_series_sorted_by_episodes_number(self,
                                             title_basics_data: BaseTSV,
                                             limit: int = 10
                                             ):
        episodeCount = 'count'
        episodeCount_alias = 'numEpisodes'
    
        return (
            self.tsv_df
                .filter(condition=(
                    col(TitleEpisodeModel.episodeNumber).isNotNull()
                ))
                .groupBy(TitleEpisodeModel.parentTconst)
                    .count()
                .orderBy(
                    col(episodeCount)
                        .desc()
                )
                .limit(num=limit)
                .select([
                    col(TitleEpisodeModel.parentTconst)
                        .alias(TitleBasicsModel.tconst),
                    col(episodeCount)
                        .alias(episodeCount_alias)
                ])
                .join(
                    other=title_basics_data.tsv_df
                        .select([
                            TitleBasicsModel.tconst,
                            TitleBasicsModel.primaryTitle
                        ]),
                    on=TitleBasicsModel.tconst,
                    how='inner'
                )
                .orderBy(
                    col(episodeCount_alias)
                        .desc()
                )
        )
        
