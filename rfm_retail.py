import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import functions as F

def create_spark_session():
    sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('RFM_Retail_Analysis')
        .setMaster('local')
        .set("spark.jars", "/opt/postgresql-42.2.18.jar")
    ))
    sparkcontext.setLogLevel("WARN")
    return pyspark.sql.SparkSession(sparkcontext.getOrCreate())

def load_configs():
    dotenv_path = Path('/resources/.env')
    load_dotenv(dotenv_path=dotenv_path)
    
    return {
        'postgres_host': os.getenv('POSTGRES_CONTAINER_NAME'),
        'postgres_dw_db': os.getenv('POSTGRES_DW_DB'),
        'postgres_user': os.getenv('POSTGRES_USER'),
        'postgres_password': os.getenv('POSTGRES_PASSWORD')
    }

def main():
    spark = create_spark_session()

    config = load_configs()
    
    jdbc_url = f'jdbc:postgresql://{config["postgres_host"]}/{config["postgres_dw_db"]}'
    jdbc_properties = {
        'user': config['postgres_user'],
        'password': config['postgres_password'],
        'driver': 'org.postgresql.Driver',
        'stringtype': 'unspecified'
    }
    
    retail_df = spark.read.jdbc(
        jdbc_url,
        'public.retail',
        properties=jdbc_properties
    )
    
    retail_df = retail_df.withColumn(
        "total_price", 
        retail_df["unitprice"] * retail_df["quantity"]
    )
    
    max_date = retail_df.agg(F.max("invoicedate")).collect()[0][0]
    
    rfm_df = retail_df.groupBy("customerid").agg(
        (F.datediff(F.lit(max_date), F.max("invoicedate"))).alias("Recency"),
        F.count("invoiceno").alias("Frequency"),
        F.sum(F.col("quantity") * F.col("unitprice")).alias("Monetary")
    )
    
    (
        rfm_df
        .write
        .mode("append")
        .option("truncate", "true")
        .jdbc(
            jdbc_url,
            'public.rfm_retail',
            properties=jdbc_properties
        )
    )
    
    spark.stop()

if __name__ == "__main__":
    main()