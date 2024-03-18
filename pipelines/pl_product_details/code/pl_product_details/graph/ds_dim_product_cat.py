from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *

def ds_dim_product_cat(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ProductCategoryKey", StringType(), True), StructField("ProductCategoryAlternateKey", StringType(), True), StructField("EnglishProductCategoryName", StringType(), True), StructField("SpanishProductCategoryName", StringType(), True), StructField("FrenchProductCategoryName", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/DimProductCategory.csv")
