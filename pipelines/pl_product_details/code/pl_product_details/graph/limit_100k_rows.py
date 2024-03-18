from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *

def limit_100k_rows(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.limit(100000)
