from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, IntegerType, StringType, TimestampType
from datetime import datetime

spark=SparkSession.builder\
                    .appName("Load-to-Silver")\
                    .getOrCreate()
bronze_df=spark.read.format("parquet")\
                    .load("/home/barnita/work/airflow-projects/dags/project-3/data/bronze/")
                    
current_timestamp=datetime.now()
end_date=datetime.strptime('2050-12-31 23:59:59', '%Y-%m-%d %H:%M:%S')

newly_inserted_df=bronze_df.dropDuplicates(["customer_id",
                        "customer_name",
                        "city",
                        "state",
                        "email"])\
                        .withColumn("is_active",lit("Y"))\
                        .withColumn("start_date",lit(current_timestamp))\
                        .withColumn("end_date",lit(end_date))\
                        .select(
                        "customer_id",
                        "customer_name",
                        "city",
                        "state",
                        "email",
                        col("is_active"),
                        col("start_date"),
                        col("end_date"))
                    
                    
                    
newly_inserted_df.write.format("parquet").mode("overwrite")\
                        .save("/home/barnita/work/airflow-projects/dags/project-3/data/silver/")
