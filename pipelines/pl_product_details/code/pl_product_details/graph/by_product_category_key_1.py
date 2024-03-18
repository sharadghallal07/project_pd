from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *

def by_product_category_key_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.ProductSubcategoryKey") == col("in1.ProductSubcategoryKey")), "inner")
