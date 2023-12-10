import pyspark
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers.BaseTSV import BaseTSV
from models import *


class TitlePrincipalsWorker(BaseTSV):
    def __init__(self,
                 spark_session: 'pyspark.sql.SparkSession',
                 path: str
                 ):
        super().__init__(spark_session, path, title_principals_schema)

    def get_titles_sorted_by_composers_number(self,
                                              title_basics_data: BaseTSV,
                                              limit: int = 10
                                              ):
        composerCount = 'count'
        composerCount_alias = 'numComposers'

        return (
            self.tsv_df
                .filter(condition=(
                    col(TitlePrincipalsModel.category) == 'composer'
                ))
                .groupBy(TitlePrincipalsModel.tconst)
                    .count()
                .select([
                    TitlePrincipalsModel.tconst,
                    col(composerCount)
                        .alias(composerCount_alias)
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
                    col(composerCount_alias)
                        .desc()
                )
                .limit(num=limit)
        )
