from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, IntegerType, StringType, TimestampType
from datetime import datetime

raw_schema=StructType([
    
    StructField("transaction_id",StringType())
    ,StructField("sales_id",StringType())
    ,StructField("customer_id",StringType())
    ,StructField("customer_name",StringType())
    ,StructField("city",StringType())
    ,StructField("state",StringType())
    ,StructField("email",StringType())
    ,StructField("order_date",DateType())
    ,StructField("order_product",StringType())
    ,StructField("quantity",IntegerType())
    ,StructField("price",DoubleType())
])

spark=SparkSession.builder\
                    .appName("Load-to-Bronze")\
                    .getOrCreate()
source_df=spark.read.format("csv")\
                .option("header","true")\
                .schema(raw_schema)\
                .load("/home/barnita/work/airflow-projects/dags/project-3/incremental-data/")

source_df.write.format("parquet").mode("overwrite")\
                        .save("/home/barnita/work/airflow-projects/dags/project-3/data/bronze/")
