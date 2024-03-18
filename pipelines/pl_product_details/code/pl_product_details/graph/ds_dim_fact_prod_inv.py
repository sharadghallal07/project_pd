from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *

def ds_dim_fact_prod_inv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ProductKey", StringType(), True), StructField("DateKey", StringType(), True), StructField("MovementDate", StringType(), True), StructField("UnitCost", StringType(), True), StructField("UnitsIn", StringType(), True), StructField("UnitsOut", StringType(), True), StructField("UnitsBalance", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/FactProductInventory.csv")
