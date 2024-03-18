from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *

def ds_dim_product_subcat(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ProductSubcategoryKey", StringType(), True), StructField("ProductSubcategoryAlternateKey", StringType(), True), StructField("EnglishProductSubcategoryName", StringType(), True), StructField("SpanishProductSubcategoryName", StringType(), True), StructField("FrenchProductSubcategoryName", StringType(), True), StructField("ProductCategoryKey", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/DimProductSubCategory.csv")
