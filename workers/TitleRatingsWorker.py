import pyspark
import pyspark.sql.types as t
from pyspark.sql.functions import col

from workers.BaseTSV import BaseTSV
from models import *


class TitleRatingsWorker(BaseTSV):
    def __init__(self,
                 spark_session: 'pyspark.sql.SparkSession',
                 path: str
                 ):
        super().__init__(spark_session, path, title_ratings_schema)

    def get_titles_sorted_by_votes_number(self,
                                          title_basics_data: BaseTSV,
                                          limit: int = 10
                                          ):
        
        return (
            self.tsv_df
                .join(
                    other=title_basics_data.tsv_df,
                    on=TitleBasicsModel.tconst,
                    how='inner'
                )
                .orderBy(
                    col(TitleRatingsModel.numberVotes)
                        .desc()
                )
                .select([
                    TitleBasicsModel.tconst,
                    TitleRatingsModel.numberVotes,
                    TitleBasicsModel.primaryTitle
                ])
                .limit(num=limit)
        )
