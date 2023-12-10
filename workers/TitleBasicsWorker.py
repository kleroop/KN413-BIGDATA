import pyspark
import pyspark.sql.types as t
from pyspark.sql.functions import col

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
    
    def get_titles_with_longest_runtime(self,
                                        limit: int = 25):
        
        return (
            self.tsv_df
                .filter(condition=(
                    (col(TitleBasicsModel.primaryTitle).isNotNull()) &
                    (col(TitleBasicsModel.runtimeMinutes).isNotNull())
                ))
                .orderBy(
                    col(TitleBasicsModel.runtimeMinutes)
                        .desc()
                )
                .select([
                    TitleBasicsModel.tconst,
                    TitleBasicsModel.titleType,
                    TitleBasicsModel.primaryTitle,
                    TitleBasicsModel.runtimeMinutes
                ])
                .limit(num=limit)
        )
