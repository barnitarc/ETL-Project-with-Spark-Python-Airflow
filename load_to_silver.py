from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, IntegerType, StringType, TimestampType
from datetime import datetime

spark=SparkSession.builder\
                    .appName("Load-to-Silver")\
                    .getOrCreate()
bronze_df=spark.read.format("parquet")\
                    .load("/home/barnita/work/airflow-projects/dags/project-3/data/bronze/")


silver_df=spark.read.format("parquet")\
                    .load("/home/barnita/work/airflow-projects/dags/project-3/data/silver/")

silver_df.createOrReplaceTempView(f"TempTable_Silver_Cleansed")


current_timestamp=datetime.now()
end_date=datetime.strptime('2050-12-31 23:59:59', '%Y-%m-%d %H:%M:%S')

#find the newly inserted data
bronze_df=bronze_df.dropDuplicates(["customer_id",
                        "customer_name",
                        "city",
                        "state",
                        "email"])
silver_df=silver_df.drop("rank")

newly_inserted_df=bronze_df.alias('a')\
                        .join(silver_df.alias('b'),col("a.customer_id")==col("b.customer_id"),"leftanti")\
                        .dropDuplicates(["customer_id",
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
                        
# find updated records

updated_df=silver_df.alias('a')\
                        .join(bronze_df.alias('b'),
                        col("a.customer_id")==col("b.customer_id"),
                        "inner")\
                        .filter(
                            (col("a.is_active")=='Y') &
                            (
                                (col("a.city")!=col("b.city") )|
                                (col("a.state")!=col("b.state")) |
                                (col("a.email")!=col("b.email")) 
                            )
                        
                        )\
                        .withColumn("is_active",lit("Y"))\
                        .withColumn("start_date",lit(current_timestamp))\
                        .withColumn("end_date",lit(end_date))\
                        .select(
                        col("b.customer_id"),
                        col("b.customer_name"), 
                        col("b.city"),
                        col("b.state"),
                        col("b.email"),
                        col("is_active"),
                        col("start_date"),
                        col("end_date")

                        )
                        
# union

joined_df=silver_df.union(updated_df).union(newly_inserted_df)


# make is_active flag 'N' fr inactive records

window=Window.partitionBy("customer_id","is_active")\
                .orderBy(col("start_date").desc())
ranked_df=joined_df.withColumn("rank",rank().over(window))

ranked_df=ranked_df.withColumn("end_date",when((col("is_active")=='Y') &
                            (col("rank")>=2)
                            ,current_timestamp).otherwise(col("end_date")))\
			.withColumn("is_active",when((col("is_active")=='Y') &
                            (col("rank")>=2)
                            ,'N').otherwise(col("is_active")))
                    
ranked_df=ranked_df.drop("rank").orderBy(col("customer_id"))

ranked_df.write.format("parquet").mode("overwrite")\
                        .save("/home/barnita/work/airflow-projects/dags/project-3/data/silver/")
