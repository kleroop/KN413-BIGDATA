import pyspark
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers.BaseTSV import BaseTSV
from models import *


class TitleAkasWorker(BaseTSV):
    def __init__(self,
                 spark_session: 'pyspark.sql.SparkSession',
                 path: str
                 ):
        super().__init__(spark_session, path, title_akas_schema)

    def get_titles_sorted_by_most_regions_aired(self,
                                                title_basics_data: BaseTSV,
                                                limit: int = 10):
        
        regionCounter = 'count'
        regionCounter_alias = 'numRegions'

        return (
            self.tsv_df
                .filter(condition=(
                    (col(TitleAkasModel.region).isNotNull())
                ))
                .groupBy(TitleAkasModel.titleId)
                    .count()
                .select([
                    col(TitleAkasModel.titleId)
                        .alias(TitleBasicsModel.tconst),
                    col(regionCounter)
                        .alias(regionCounter_alias)
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
                    col(regionCounter_alias)
                        .desc()
                )
                .limit(num=limit)
        )
