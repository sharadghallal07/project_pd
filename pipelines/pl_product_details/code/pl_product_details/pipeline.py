from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_product_details.config.ConfigStore import *
from pl_product_details.udfs.UDFs import *
from prophecy.utils import *
from pl_product_details.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_dim_fact_prod_inv = ds_dim_fact_prod_inv(spark)
    df_ds_dim_product = ds_dim_product(spark)
    df_ds_dim_product_cat = ds_dim_product_cat(spark)
    df_ds_dim_product_subcat = ds_dim_product_subcat(spark)
    df_by_product_category_key = by_product_category_key(spark, df_ds_dim_product_cat, df_ds_dim_product_subcat)
    df_by_product_category_key_1 = by_product_category_key_1(spark, df_by_product_category_key, df_ds_dim_product)
    df_by_product_key_outer_join = by_product_key_outer_join(
        spark, 
        df_ds_dim_fact_prod_inv, 
        df_by_product_category_key_1
    )
    df_keeping_all_data = keeping_all_data(spark, df_by_product_key_outer_join)
    df_limit_100k_rows = limit_100k_rows(spark, df_keeping_all_data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_product_details")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_product_details", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_product_details")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
