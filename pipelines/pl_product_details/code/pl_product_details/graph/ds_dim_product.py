from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *

def ds_dim_product(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ProductKey", StringType(), True), StructField("ProductAlternateKey", StringType(), True), StructField("ProductSubcategoryKey", StringType(), True), StructField("WeightUnitMeasureCode", StringType(), True), StructField("SizeUnitMeasureCode", StringType(), True), StructField("EnglishProductName", StringType(), True), StructField("SpanishProductName", StringType(), True), StructField("FrenchProductName", StringType(), True), StructField("StandardCost", StringType(), True), StructField("FinishedGoodsFlag", StringType(), True), StructField("Color", StringType(), True), StructField("SafetyStockLevel", StringType(), True), StructField("ReorderPoint", StringType(), True), StructField("ListPrice", StringType(), True), StructField("Size", StringType(), True), StructField("SizeRange", StringType(), True), StructField("Weight", StringType(), True), StructField("DaysToManufacture", StringType(), True), StructField("ProductLine", StringType(), True), StructField("DealerPrice", StringType(), True), StructField("Class", StringType(), True), StructField("Style", StringType(), True), StructField("ModelName", StringType(), True), StructField("LargePhoto", StringType(), True), StructField("EnglishDescription", StringType(), True), StructField("FrenchDescription", StringType(), True), StructField("ChineseDescription", StringType(), True), StructField("ArabicDescription", StringType(), True), StructField("HebrewDescription", StringType(), True), StructField("ThaiDescription", StringType(), True), StructField("GermanDescription", StringType(), True), StructField("JapaneseDescription", StringType(), True), StructField("TurkishDescription", StringType(), True), StructField("StartDate", StringType(), True), StructField("EndDate", StringType(), True), StructField("Status", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/DimProduct.csv")
